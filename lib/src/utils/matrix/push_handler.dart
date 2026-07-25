import 'package:mutex/mutex.dart';


import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';

import 'package:flutter_local_notifications/flutter_local_notifications.dart';
import 'package:http/http.dart' as http;
import 'package:matrix/matrix.dart';
import 'package:unifiedpush/unifiedpush.dart';
import 'package:unifiedpush_platform_interface/unifiedpush_platform_interface.dart';

import '../../../l10n/generated/app_localizations.dart';
import '../../../l10n/matrix/polycule_matrix_localizations.dart';
import '../../widgets/matrix/client_manager/client_store.dart';
import '../polycule_http_client/polycule_http_client.dart';
import '../settings_interface.dart';
import '../unified_push/unified_push_storage_polycule.dart';
import 'active_room_tracker.dart';
import 'cached_push_rules.dart';
import 'call_event_summary.dart';
import 'client_util.dart';
import 'database/matrix_store_lease.dart';
import 'is_display_event_extension.dart';
import 'push_manager.dart';
import 'poll_event.dart';
import 'push_log_journal.dart';
import 'voip/call_notification_manager.dart';
import 'voip/call_log_journal.dart';

const _headlessStorageTimeout = Duration(seconds: 5);
const _headlessRequestTimeout = Duration(seconds: 15);
const _backgroundNotificationDeadline = Duration(seconds: 20);
const backgroundFastFallbackDelay = Duration(seconds: 4);

Future<void> _cancelBackgroundFallback(
  FlutterLocalNotificationsPlugin plugin,
  String roomId,
) =>
    plugin.cancel(
      PushManager.backgroundFallbackId(roomId),
      tag: PushManager.backgroundFallbackTag,
    );

@pragma('vm:entry-point')
Future<void> pushEntrypoint() async {
  // The background isolate does not execute the foreground `main()` path.
  // Initialize the crypto implementation here as well, otherwise Client.init
  // cannot restore the stored Olm/Megolm session and every encrypted push is
  // rendered as a generic notification.
  await ClientUtil.initVodozemac();
  await UnifiedPush.initialize(
    onMessage: _queueBackgroundNotification,
    linuxOptions: LinuxOptions(
      dbusName: 'business.braid.polycule',
      storage: UnifiedPushStoragePolycule(),
      background: true,
    ),
  );
}

void _queueBackgroundNotification(dynamic message, dynamic instance) {
  _processPushSafely(message, instance);
}

enum PushNotificationResult { shown, suppressed, unresolved }

class _BackgroundDeadlineExceeded implements Exception {}

/// Lets the Matrix SDK restore its local session without issuing the
/// unconditional `/sync` request at the end of [Client.init].
class _HeadlessSessionRestoreClient extends http.BaseClient {
  _HeadlessSessionRestoreClient(this.inner, this.nextBatch);

  final http.Client inner;
  final String nextBatch;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    if (request.url.path.endsWith('/sync')) {
      final bytes = utf8.encode(jsonEncode({'next_batch': nextBatch}));
      return Future.value(
        http.StreamedResponse(
          Stream.value(bytes),
          200,
          contentLength: bytes.length,
          headers: const {'content-type': 'application/json'},
          request: request,
        ),
      );
    }
    return inner.send(request);
  }

  @override
  void close() {
    // The wrapped client remains owned by the Matrix Client and is restored
    // immediately after local session initialization.
  }
}

class BackgroundNotificationState {
  Future<void> _mutation = Future.value();
  bool _fallbackShown = false;
  bool _settled = false;

  Future<T> _synchronized<T>(Future<T> Function() action) async {
    final previous = _mutation;
    final gate = Completer<void>();
    _mutation = gate.future;
    await previous;
    try {
      return await action();
    } finally {
      gate.complete();
    }
  }

  Future<bool> showFallback(Future<bool> Function() show) =>
      _synchronized(() async {
        if (_settled || _fallbackShown) {
          return false;
        }
        return _fallbackShown = await show();
      });

  Future<void> showComplete({
    required Future<void> Function() cancelFallback,
    required Future<void> Function() show,
  }) =>
      _synchronized(() async {
        if (_settled) {
          return;
        }
        if (_fallbackShown) {
          await cancelFallback();
          _fallbackShown = false;
        }
        await show();
        _settled = true;
      });

  Future<void> suppress(
    Future<void> Function(bool fallbackShown) settle,
  ) =>
      _synchronized(() async {
        if (_settled) {
          return;
        }
        await settle(_fallbackShown);
        _fallbackShown = false;
        _settled = true;
      });
}

@pragma('vm:entry-point')
Future<void> handleBackgroundNotification(
  PushMessage message,
  String instance,
) async {
  final stopwatch = Stopwatch()..start();
  WidgetsFlutterBinding.ensureInitialized();
  final locale =
      WidgetsBinding.instance.platformDispatcher.computePlatformResolvedLocale(
            AppLocalizations.supportedLocales,
          ) ??
          const Locale('en');
  final l10n = await AppLocalizations.delegate.load(locale);
  Client? client;
  MatrixStoreLease? storeLease;
  Set<String>? mutedRoomIds;
  Timer? fastFallbackTimer;
  final notificationState = BackgroundNotificationState();

  if (await _handleCallSignalingPush(
    message: message.content,
    instance: instance,
    l10n: l10n,
  )) {
    await PushLogJournal.record(
      'Handled call signaling without opening the Matrix database.',
    );
    return;
  }
  await PushLogJournal.record('Received background callback for $instance.');

  Future<bool> showFallback() async {
    final shown = await notificationState.showFallback(
      () => _showBackgroundFallbackNotification(
        instance: instance,
        client: client,
        l10n: l10n,
        message: message.content,
        mutedRoomIds: mutedRoomIds,
      ),
    );
    if (shown) {
      await PushLogJournal.record(
        'Background fallback shown after '
        '${stopwatch.elapsedMilliseconds}ms.',
        important: true,
      );
    }
    return shown;
  }

  fastFallbackTimer = Timer(
    backgroundFastFallbackDelay,
    () => unawaited(showFallback()),
  );

  final fullNotification = () async {
    storeLease = await MatrixStoreLease.acquire();
    Logs().d('Loading headless network settings.');
    final settings = await const SettingsInterface()
        .getNetwork(failClosed: true)
        .timeout(_headlessStorageTimeout);
    await PushLogJournal.record(
      settings.useSocks5Proxy
          ? 'Network settings loaded: SOCKS5 required.'
          : 'Headless Matrix lookup uses the direct/system network path.',
    );
    await PolyculeHttpClientManager.init(
      ValueNotifier(settings),
    ).timeout(_headlessStorageTimeout);
    final httpCallback = await PolyculeHttpClientManager.httpClientCallback
        .timeout(_headlessStorageTimeout);
    await PushLogJournal.record('HTTP client initialized.');

    await PushLogJournal.record(
      'Opening Matrix database.',
      level: Level.debug,
    );
    client = await ClientUtil.clientConstructor(
      instance,
      httpCallback.call(),
      requestTimeout: _headlessRequestTimeout,
    );
    mutedRoomIds = await prepareHeadlessPushClient(client!);
    await PushLogJournal.record(
      'Matrix session prepared; cached muted rooms: ${mutedRoomIds!.length}.',
      level: Level.debug,
    );
    await PushLogJournal.record('Looking up Matrix event.');
    Future<PushNotificationResult> resolveNotification() =>
        handlePushNotification(
          client: client!,
          l10n: l10n,
          message: message.content,
          mutedRoomIds: mutedRoomIds!,
          performClearingSync: false,
          eventLookupTimeout: _headlessRequestTimeout,
          publishShortcut: true,
          backgroundState: notificationState,
        );

    PushNotificationResult result;
    try {
      result = await resolveNotification();
    } on MatrixException catch (error) {
      if (error.error != MatrixError.M_UNKNOWN_TOKEN) {
        rethrow;
      }
      await PushLogJournal.record(
        'Homeserver rejected the access token; refreshing the stored '
        'session and retrying once.',
        level: Level.warning,
      );
      await _refreshHeadlessPushAccessToken(
        client!,
        rejectedAccessToken: client!.accessToken,
      );
      result = await resolveNotification();
    }
    await PushLogJournal.record(
      'Matrix notification handling result: ${result.name}.',
    );
    return result;
  }();

  try {
    final remaining = _backgroundNotificationDeadline - stopwatch.elapsed;
    if (remaining.isNegative) {
      throw _BackgroundDeadlineExceeded();
    }
    final result = await fullNotification.timeout(
      remaining,
      onTimeout: () => throw _BackgroundDeadlineExceeded(),
    );
    if (result == PushNotificationResult.unresolved) {
      await showFallback();
    }
  } on _BackgroundDeadlineExceeded catch (error, stackTrace) {
    await PushLogJournal.record(
      'Full background notification exceeded 10 seconds.',
      level: Level.warning,
      error: error,
      stackTrace: stackTrace,
    );
    await showFallback();
    try {
      await fullNotification;
    } catch (error, stackTrace) {
      await PushLogJournal.record(
        'Late background notification lookup failed.',
        level: Level.error,
        error: error,
        stackTrace: stackTrace,
      );
      await showFallback();
    }
  } catch (error, stackTrace) {
    await PushLogJournal.record(
      'Background notification failed.',
      level: Level.error,
      error: error,
      stackTrace: stackTrace,
    );
    await showFallback();
  } finally {
    fastFallbackTimer.cancel();
    final clientToDispose = client;
    if (clientToDispose != null) {
      try {
        await clientToDispose.dispose().timeout(_headlessStorageTimeout);
      } catch (error, stackTrace) {
        Logs()
            .w('Failed to dispose headless Matrix client.', error, stackTrace);
      }
    }
    await storeLease?.release();
    await PushLogJournal.record(
      'Background notification finished in ${stopwatch.elapsedMilliseconds}ms.',
    );
  }
}

Future<bool> _handleCallSignalingPush({
  required Uint8List message,
  required String instance,
  required AppLocalizations l10n,
}) async {
  PushNotification notification;
  try {
    notification = decodeMessage(message);
  } catch (_) {
    return false;
  }
  final type = notification.type;
  if (type == null || !isMatrixCallSignalingEventType(type)) {
    return false;
  }

  final content = notification.content ?? const <String, Object?>{};
  final callId = content['call_id'];
  final terminal = type.endsWith('.hangup') || type.endsWith('.reject');
  if (terminal) {
    if (callId is String) {
      await CallNotificationManager.cancel(callId);
    }
    return true;
  }

  if (!type.endsWith('.invite')) {
    return true;
  }
  final roomId = notification.roomId;
  final clientIdentifier = int.tryParse(
    RegExp(r'(\d+)$').firstMatch(instance)?.group(1) ?? '',
  );
  if (callId is! String || roomId == null || clientIdentifier == null) {
    await CallLogJournal.record(
      'Incoming call push lacked required public routing metadata.',
      level: Level.warning,
      important: true,
    );
    return true;
  }

  final rawLifetime = content['lifetime'];
  final lifetimeMs = rawLifetime is int
      ? rawLifetime.clamp(1000, const Duration(minutes: 2).inMilliseconds)
      : const Duration(minutes: 1).inMilliseconds;
  await CallNotificationManager.showIncoming(
    clientIdentifier: clientIdentifier,
    roomId: roomId,
    callId: callId,
    callerName:
        notification.senderDisplayName ?? notification.roomName ?? l10n.appName,
    video: callInviteContainsVideo(content),
    timeout: Duration(milliseconds: lifetimeMs),
  );
  return true;
}

Future<bool> _showBackgroundFallbackNotification({
  required String instance,
  required Client? client,
  required AppLocalizations l10n,
  required Uint8List message,
  required Set<String>? mutedRoomIds,
}) async {
  try {
    final notification = decodeMessage(message);
    final roomId = notification.roomId;
    if (roomId == null || mutedRoomIds?.contains(roomId) == true) {
      return false;
    }
    if (mutedRoomIds == null) {
      Logs().w(
        'Showing fallback without cached mute rules for $instance.',
      );
    }

    await PushManager.initializeNotificationPlugin(
      l10n,
      processLaunchDetails: false,
    ).timeout(_headlessStorageTimeout);
    final plugin = FlutterLocalNotificationsPlugin();
    final roomName = notification.roomName ?? l10n.appName;
    final envelopeBody = fallbackNotificationBody(notification, l10n);
    if (!kIsWeb && Platform.isAndroid) {
      await plugin
          .resolvePlatformSpecificImplementation<
              AndroidFlutterLocalNotificationsPlugin>()
          ?.createNotificationChannel(
            AndroidNotificationChannel(
              roomId,
              roomName,
              importance: Importance.high,
            ),
          );
    }
    final clientIdentifier = client?.clientName.clientIdentifier ??
        int.tryParse(RegExp(r'(\d+)$').firstMatch(instance)?.group(1) ?? '');
    await plugin.show(
      PushManager.backgroundFallbackId(roomId),
      roomName,
      envelopeBody,
      NotificationDetails(
        android: AndroidNotificationDetails(
          roomId,
          roomName,
          tag: PushManager.backgroundFallbackTag,
          importance: Importance.high,
          priority: Priority.max,
          category: AndroidNotificationCategory.message,
          icon: '@drawable/ic_launcher_foreground',
        ),
        iOS: const DarwinNotificationDetails(),
      ),
      payload: clientIdentifier == null
          ? null
          : jsonEncode({'client': clientIdentifier, 'room': roomId}),
    );
    return true;
  } catch (error, stackTrace) {
    await PushLogJournal.record(
      'Fallback notification failed.',
      level: Level.error,
      error: error,
      stackTrace: stackTrace,
    );
    return false;
  }
}

@visibleForTesting
String fallbackNotificationBody(
  PushNotification notification,
  AppLocalizations l10n,
) {
  if (notification.type == EventTypes.Message) {
    final body = notification.content?['body'];
    if (body is String && body.trim().isNotEmpty) {
      return body.trim();
    }
  }
  return l10n.newNotification;
}

Future<Set<String>> prepareHeadlessPushClient(Client client) async {
  var account = await client.database.getClient(client.clientName);
  final savedHttpClient = client.httpClient;
  final previousBatch = account?['prev_batch'];

  // Client.init is the SDK-owned path that restores user/device identity,
  // rooms and the encrypted Olm account from the local database. Disable its
  // sync loop before initialization. The SDK unconditionally performs one
  // zero-timeout sync at the end of init, so satisfy only that request from a
  // local response which preserves the stored sync token.
  client.backgroundSync = false;
  client.httpClient = _HeadlessSessionRestoreClient(
    savedHttpClient,
    previousBatch is String ? previousBatch : '',
  );
  try {
    await client.init(
      waitForFirstSync: true,
      waitUntilLoadCompletedLoaded: true,
    );
  } finally {
    client.backgroundSync = false;
    client.httpClient = savedHttpClient;
  }

  account = await client.database.getClient(client.clientName);
  final homeserver = account?['homeserver_url'];
  final accessToken = account?['token'];
  if (homeserver is! String || accessToken is! String) {
    throw StateError('No stored Matrix session for ${client.clientName}.');
  }

  client
    ..homeserver = Uri.parse(homeserver)
    ..accessToken = accessToken;

  final rawTokenExpiresAt = account?['token_expires_at'];
  final tokenExpiresAtMs = int.tryParse(
    rawTokenExpiresAt is String ? rawTokenExpiresAt : '',
  );
  final tokenExpiresAt = tokenExpiresAtMs == null
      ? null
      : DateTime.fromMillisecondsSinceEpoch(tokenExpiresAtMs);
  if (tokenExpiresAt != null &&
      tokenExpiresAt.difference(DateTime.now()) <= const Duration(minutes: 1) &&
      account?['refresh_token'] is String) {
    await PushLogJournal.record(
      'Stored Matrix access token is expired or near expiry; refreshing it.',
    );
    await _refreshHeadlessPushAccessToken(
      client,
      rejectedAccessToken: accessToken,
    );
  }

  final accountData = await client.database.getAccountData();
  return mutedRoomIdsFromPushRules(
    accountData[EventTypes.PushRules]?.content,
  );
}

Future<void> _refreshHeadlessPushAccessToken(
  Client client, {
  required String? rejectedAccessToken,
}) async {
  final account = await client.database.getClient(client.clientName);
  if (account == null) {
    throw StateError('No stored Matrix session for ${client.clientName}.');
  }

  final storedAccessToken = account['token'];
  if (storedAccessToken is! String) {
    throw StateError('Stored Matrix session has no access token.');
  }

  // A foreground client may have rotated the token after this headless
  // client was created. Prefer that token instead of rotating twice.
  if (rejectedAccessToken != null && storedAccessToken != rejectedAccessToken) {
    client.accessToken = storedAccessToken;
    await PushLogJournal.record(
      'A newer Matrix access token was already stored; using it for retry.',
    );
    return;
  }

  final homeserver = account['homeserver_url'];
  final refreshToken = account['refresh_token'];
  final userId = account['user_id'];
  if (homeserver is! String || refreshToken is! String || userId is! String) {
    throw StateError('Stored Matrix session cannot be refreshed.');
  }

  final oidcClientId = account['oidc_dynamic_client_id'];
  final rawOidcMetadata = account['oidc_auth_metadata'];
  Map<String, Object?>? oidcMetadata;
  if (rawOidcMetadata is String) {
    final decoded = jsonDecode(rawOidcMetadata);
    if (decoded is Map) {
      oidcMetadata = Map<String, Object?>.from(decoded);
    }
  }

  late final String newAccessToken;
  late final String newRefreshToken;
  late final DateTime? newTokenExpiresAt;
  final oidcTokenEndpoint = oidcMetadata?['token_endpoint'];
  if (oidcTokenEndpoint is String && oidcClientId is String) {
    final response = await client.oidcRefreshToken(
      tokenEndpoint: Uri.parse(oidcTokenEndpoint),
      refreshToken: refreshToken,
      oidcClientId: oidcClientId,
    );
    newAccessToken = response.accessToken;
    newRefreshToken = response.refreshToken;
    newTokenExpiresAt = DateTime.now().add(
      Duration(seconds: response.expiresIn),
    );
  } else {
    final response = await client.refresh(refreshToken);
    newAccessToken = response.accessToken;
    newRefreshToken = response.refreshToken ?? refreshToken;
    newTokenExpiresAt = response.expiresInMs == null
        ? null
        : DateTime.now().add(Duration(milliseconds: response.expiresInMs!));
  }

  client.accessToken = newAccessToken;
  await client.database.updateClient(
    homeserver,
    newAccessToken,
    newTokenExpiresAt,
    newRefreshToken,
    userId,
    account['device_id'] as String?,
    account['device_name'] as String?,
    account['prev_batch'] as String?,
    account['olm_account'] as String?,
  );
  await PushLogJournal.record('Matrix access token refreshed successfully.');
}

Future<PushNotificationResult> handlePushNotification({
  required Client client,
  required AppLocalizations l10n,
  required Uint8List message,
  Set<String> mutedRoomIds = const {},
  bool performClearingSync = true,
  Duration eventLookupTimeout = const Duration(seconds: 8),
  bool publishShortcut = true,
  BackgroundNotificationState? backgroundState,
}) async {
  final notification = decodeMessage(message);

  WidgetsFlutterBinding.ensureInitialized();

  final notificationsPlugin = FlutterLocalNotificationsPlugin();
  await PushManager.initializeNotificationPlugin(l10n);

  // this code is mostly based on FluffyChat's implementation - huge credits
  final event = await client.getEventByPushNotification(
    notification,
    storeInDatabase: false,
    timeoutForServerRequests: eventLookupTimeout,
  );

  if (backgroundState != null && event?.type == EventTypes.Encrypted) {
    await PushLogJournal.record(
      'Encrypted push could not be decrypted with the locally stored session.',
      level: Level.warning,
    );
  }

  if (event == null) {
    Logs().v('Notification is a clearing indicator.');
    if (notification.counts?.unread == null ||
        notification.counts?.unread == 0) {
      if (backgroundState == null) {
        await notificationsPlugin.cancelAll();
      } else {
        await backgroundState.suppress((_) => notificationsPlugin.cancelAll());
      }
    } else if (performClearingSync) {
      await client.roomsLoading;
      await client.oneShotSync();
      final activeNotifications =
          await notificationsPlugin.getActiveNotifications();
      for (final activeNotification in activeNotifications) {
        final room = client.rooms
            .where((room) => room.id.hashCode == activeNotification.id)
            .singleOrNull;
        if (room == null || !room.isUnreadOrInvited) {
          await notificationsPlugin.cancel(activeNotification.id!);
        }
      }
    } else {
      return PushNotificationResult.unresolved;
    }
    return PushNotificationResult.suppressed;
  }

  if (isMatrixCallSignalingEventType(event.type)) {
    final callId = event.content['call_id'];
    final isInvite = event.type.endsWith('.invite');
    final isTerminal =
        event.type.endsWith('.hangup') || event.type.endsWith('.reject');

    if (callId is String && isTerminal) {
      await CallNotificationManager.cancel(callId);
    }

    if (callId is String && isInvite && event.senderId != client.userID) {
      await CallLogJournal.record(
        'UnifiedPush resolved an incoming Matrix call invite.',
        important: true,
      );
      final rawLifetime = event.content['lifetime'];
      final lifetime = rawLifetime is int
          ? rawLifetime
          : const Duration(minutes: 1).inMilliseconds;
      final unsignedAge = event.unsigned?.tryGet<int>('age');
      final age = unsignedAge ??
          DateTime.now().difference(event.originServerTs).inMilliseconds;
      final maximumLifetime = lifetime < 1000 ? 1000 : lifetime;
      final remaining = Duration(
        milliseconds: (lifetime - age).clamp(1000, maximumLifetime).toInt(),
      );
      final sender = event.senderFromMemoryOrFallback;
      Future<void> show() => CallNotificationManager.showIncoming(
            clientIdentifier: client.clientName.clientIdentifier,
            roomId: event.room.id,
            callId: callId,
            callerName: sender.calcDisplayname(i18n: l10n.matrix),
            video: callInviteContainsVideo(event.content),
            timeout: remaining,
          );
      if (backgroundState == null) {
        await show();
      } else {
        await backgroundState.showComplete(
          cancelFallback: () => _cancelBackgroundFallback(
            notificationsPlugin,
            event.room.id,
          ),
          show: show,
        );
      }
      return PushNotificationResult.shown;
    }

    if (backgroundState != null) {
      await backgroundState.suppress((fallbackShown) async {
        if (fallbackShown) {
          await _cancelBackgroundFallback(
            notificationsPlugin,
            event.room.id,
          );
        }
      });
    }
    return PushNotificationResult.suppressed;
  }

  final roomId = event.roomId;
  if (roomId != null && ActiveRoomTracker.isVisible(roomId)) {
    if (backgroundState == null) {
      await notificationsPlugin.cancel(roomId.hashCode);
    } else {
      await backgroundState.suppress((fallbackShown) async {
        if (fallbackShown) {
          await _cancelBackgroundFallback(notificationsPlugin, roomId);
        }
        await notificationsPlugin.cancel(roomId.hashCode);
      });
    }
    return PushNotificationResult.suppressed;
  }

  if (mutedRoomIds.contains(event.room.id) ||
      event.room.pushRuleState == PushRuleState.dontNotify) {
    if (backgroundState == null) {
      await notificationsPlugin.cancel(event.room.id.hashCode);
    } else {
      await backgroundState.suppress((fallbackShown) async {
        if (fallbackShown) {
          await _cancelBackgroundFallback(
            notificationsPlugin,
            event.room.id,
          );
        }
        await notificationsPlugin.cancel(event.room.id.hashCode);
      });
    }
    return PushNotificationResult.suppressed;
  }

  if (!event.shouldDisplayEvent) {
    if (backgroundState == null) {
      return PushNotificationResult.suppressed;
    } else {
      await backgroundState.suppress((fallbackShown) async {
        if (fallbackShown) {
          await _cancelBackgroundFallback(
            notificationsPlugin,
            event.room.id,
          );
        }
      });
    }
    return PushNotificationResult.suppressed;
  }

  final id = event.room.id.hashCode;

  final body = event.type == EventTypes.Encrypted
      ? l10n.newNotification
      : event.isPollStart
          ? 'Poll: ${event.pollQuestion ?? 'Poll'}'
          : await event.calcLocalizedBody(
              l10n.matrix,
              plaintextBody: true,
              withSenderNamePrefix: false,
              hideReply: true,
              hideEdit: true,
              removeMarkdown: true,
            );
  final messagingStyleInformation = !kIsWeb && Platform.isAndroid
      ? await AndroidFlutterLocalNotificationsPlugin()
          .getActiveNotificationMessagingStyle(id)
      : null;

  final sender = event.senderFromMemoryOrFallback;
  final senderAvatarFuture = sender.avatarUrl?.downloadAndroidAvatar(client);
  final roomAvatarFuture = event.room.avatar?.downloadAndroidAvatar(client);
  final senderAvatarBytes = await senderAvatarFuture;
  final roomAvatarBytes = await roomAvatarFuture;

  final person = Person(
    bot: event.messageType == MessageTypes.Notice,
    important: event.room.isFavourite,
    key: event.senderId,
    icon: senderAvatarBytes == null
        ? null
        : ByteArrayAndroidIcon(senderAvatarBytes),
    name: sender.calcDisplayname(i18n: l10n.matrix),
  );

  final newMessage = Message(body, event.originServerTs, person);

  messagingStyleInformation?.messages?.add(newMessage);

  final roomName = event.room.getLocalizedDisplayname(l10n.matrix);

  final notificationGroupId =
      event.room.isDirectChat ? 'directChats' : 'groupChats';
  final groupName = event.room.isDirectChat ? l10n.directChats : l10n.groups;

  if (Platform.isAndroid && publishShortcut) {
    try {
      const channel = MethodChannel('polycule.shortcuts');
      await channel.invokeMethod('publishConversationShortcut', {
        'id': event.room.id,
        'name': roomName,
        'personName': sender.calcDisplayname(i18n: l10n.matrix),
        'personKey': event.senderId,
        'important': event.room.isFavourite,
        'avatarBytes': roomAvatarBytes,
      });
    } catch (e) {
      Logs().w('Failed to publish conversation shortcut', e);
    }
  }

  final messageRooms = AndroidNotificationChannelGroup(
    notificationGroupId,
    groupName,
  );
  final roomsChannel = AndroidNotificationChannel(
    event.room.id,
    roomName,
    groupId: notificationGroupId,
    importance: Importance.high,
  );

  await notificationsPlugin
      .resolvePlatformSpecificImplementation<
          AndroidFlutterLocalNotificationsPlugin>()
      ?.createNotificationChannelGroup(messageRooms);
  await notificationsPlugin
      .resolvePlatformSpecificImplementation<
          AndroidFlutterLocalNotificationsPlugin>()
      ?.createNotificationChannel(roomsChannel);

  final androidPlatformChannelSpecifics = AndroidNotificationDetails(
    event.room.id,
    roomName,
    number: notification.counts?.unread,
    category: AndroidNotificationCategory.message,
    icon: '@drawable/ic_launcher_foreground',
    largeIcon: roomAvatarBytes == null
        ? null
        : ByteArrayAndroidBitmap(roomAvatarBytes),
    shortcutId: event.room.id,
    styleInformation: messagingStyleInformation ??
        MessagingStyleInformation(
          person,
          htmlFormatContent: true,
          conversationTitle: roomName,
          groupConversation: !event.room.isDirectChat,
          messages: [newMessage],
        ),
    ticker: event.isPollStart
        ? 'Poll: ${event.pollQuestion ?? 'Poll'}'
        : event.calcLocalizedBodyFallback(
            l10n.matrix,
            plaintextBody: true,
            withSenderNamePrefix: true,
            hideReply: true,
            hideEdit: true,
            removeMarkdown: true,
          ),
    importance: Importance.high,
    priority: Priority.max,
    groupKey: notificationGroupId,
  );
  const iOSPlatformChannelSpecifics = DarwinNotificationDetails();
  final platformChannelSpecifics = NotificationDetails(
    android: androidPlatformChannelSpecifics,
    iOS: iOSPlatformChannelSpecifics,
  );

  final title = event.room.getLocalizedDisplayname(l10n.matrix);

  Future<void> show() => notificationsPlugin.show(
        id,
        title,
        body,
        platformChannelSpecifics,
        payload: jsonEncode({
          'client': client.clientName.clientIdentifier,
          'room': event.roomId,
        }),
      );
  if (backgroundState == null) {
    await show();
  } else {
    await backgroundState.showComplete(
      cancelFallback: () async {
        await _cancelBackgroundFallback(
          notificationsPlugin,
          event.room.id,
        );
        await notificationsPlugin.cancel(id);
      },
      show: show,
    );
  }
  return PushNotificationResult.shown;
}

PushNotification decodeMessage(Uint8List message) {
  final content = utf8.decode(message);
  final json = jsonDecode(content);
  final data = Map<String, dynamic>.from(json['notification']);
  return PushNotification.fromJson(data);
}

extension GetAndroidIcon on Uri {
  Future<Uint8List?> downloadAndroidAvatar(Client client) async {
    try {
      final database = client.database;
      final original = await database.getFile(this);
      if (original != null) {
        return original;
      }
      final thumbnailUri = await getThumbnailUri(
        client,
        width: 128,
        height: 128,
        method: ThumbnailMethod.crop,
      ).timeout(const Duration(milliseconds: 750));
      if (thumbnailUri.host.isEmpty) {
        return null;
      }
      final thumbnail = await database.getFile(thumbnailUri);
      if (thumbnail != null) {
        return thumbnail;
      }
      final response = await client.httpClient.get(
        thumbnailUri,
        headers: {'authorization': 'Bearer ${client.accessToken}'},
      ).timeout(const Duration(milliseconds: 1250));
      if (response.statusCode != 200 || response.bodyBytes.isEmpty) {
        return null;
      }
      final bytes = response.bodyBytes;
      if (bytes.length < database.maxFileSize) {
        await database.storeFile(
          thumbnailUri,
          bytes,
          DateTime.now().millisecondsSinceEpoch,
        );
      }
      return bytes;
    } catch (error) {
      Logs().v('Unable to resolve an Android conversation avatar: $error');
      return null;
    }
  }
}



// --- INIZIO PATCH MUTEX ---
final Mutex _backgroundPushMutex = Mutex();

Future _processPushSafely(dynamic message, dynamic instance) async {
  await _backgroundPushMutex.protect(() async {
    try {
      await handleBackgroundNotification(message, instance);
    } catch (e, stackTrace) {
      print('[UnifiedPush] Errore Mutex: $e\n$stackTrace');
    }
  });
}
// --- FINE PATCH MUTEX ---
