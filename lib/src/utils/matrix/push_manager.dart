import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';

import 'package:flutter_local_notifications/flutter_local_notifications.dart';
import 'package:matrix/matrix.dart';
import 'package:unifiedpush/unifiedpush.dart';
import 'package:unifiedpush_platform_interface/unifiedpush_platform_interface.dart';

import '../../../l10n/generated/app_localizations.dart';
import '../../widgets/intent_manager.dart';
import '../settings_interface.dart';
import '../unified_push/unified_push_storage_polycule.dart';
import 'push_gateway_extension.dart';
import 'push_handler.dart';
import 'push_log_journal.dart';
import 'voip/call_notification_manager.dart';

final pusherDataMessageFormat = kIsWeb
    ? null
    : Platform.isAndroid
        ? 'android'
        : Platform.isIOS
            ? 'ios'
            : Platform.isLinux
                ? 'android'
                : null;

class PushManager {
  PushManager(this.client) {
    unawaited(_initialize());
  }

  static Future<void>? _notificationInitialization;
  static Future<void>? _fallbackDismissal;
  static const backgroundFallbackTag = 'polycule.unifiedpush.fallback';

  static int backgroundFallbackId(String roomId) =>
      roomId.hashCode ^ 0x40000000;

  static Future<void> dismissBackgroundFallbackNotifications() {
    final current = _fallbackDismissal;
    if (current != null) return current;
    final dismissal = _dismissBackgroundFallbackNotifications();
    _fallbackDismissal = dismissal;
    return dismissal.whenComplete(() {
      if (identical(_fallbackDismissal, dismissal)) {
        _fallbackDismissal = null;
      }
    });
  }

  static Future<void> _dismissBackgroundFallbackNotifications() async {
    if (kIsWeb || !Platform.isAndroid) return;
    try {
      final plugin = FlutterLocalNotificationsPlugin();
      final active = await plugin.getActiveNotifications();
      final fallbacks = active.where((notification) {
        if (notification.id == null) return false;
        if (notification.tag == backgroundFallbackTag) return true;
        final roomId = notification.channelId;
        return notification.tag == null &&
            roomId != null &&
            notification.id == backgroundFallbackId(roomId);
      });
      var dismissed = 0;
      for (final fallback in fallbacks) {
        await plugin.cancel(fallback.id!, tag: fallback.tag);
        dismissed++;
      }
      if (dismissed > 0) {
        await PushLogJournal.record(
          'Dismissed $dismissed generic notification(s) on foreground.',
        );
      }
    } catch (error, stackTrace) {
      await PushLogJournal.record(
        'Unable to dismiss generic notifications on foreground.',
        level: Level.warning,
        error: error,
        stackTrace: stackTrace,
      );
    }
  }

  static Future<void> initializeNotificationPlugin(
    AppLocalizations l10n, {
    bool processLaunchDetails = true,
  }) {
    final current = _notificationInitialization;
    if (current != null) {
      return current;
    }
    final initialization = _initializeNotificationPlugin(
      l10n,
      processLaunchDetails: processLaunchDetails,
    );
    _notificationInitialization = initialization;
    initialization.catchError((Object _) {
      if (identical(_notificationInitialization, initialization)) {
        _notificationInitialization = null;
      }
    });
    return initialization;
  }

  static Future<void> _initializeNotificationPlugin(
    AppLocalizations l10n, {
    required bool processLaunchDetails,
  }) async {
    final notificationsPlugin = FlutterLocalNotificationsPlugin();

    await notificationsPlugin
        .initialize(
          InitializationSettings(
            android: const AndroidInitializationSettings(
              '@drawable/ic_launcher_foreground',
            ),
            linux: LinuxInitializationSettings(
              defaultActionName: l10n.view,
              defaultIcon: ThemeLinuxIcon('business.braid.polycule'),
            ),
            iOS: const DarwinInitializationSettings(),
            macOS: const DarwinInitializationSettings(),
          ),
          onDidReceiveNotificationResponse: _handleNotificationResponse,
        )
        .timeout(const Duration(seconds: 5));

    if (processLaunchDetails) {
      final launchDetails =
          await notificationsPlugin.getNotificationAppLaunchDetails();
      if (launchDetails?.didNotificationLaunchApp == true) {
        final response = launchDetails?.notificationResponse;
        _openNotificationPayload(
          response?.payload,
          actionId: response?.actionId,
        );
      }
    }
  }

  static void _handleNotificationResponse(NotificationResponse response) {
    _openNotificationPayload(response.payload, actionId: response.actionId);
  }

  static void _openNotificationPayload(
    String? payload, {
    String? actionId,
  }) {
    if (payload == null) {
      return;
    }
    try {
      final data = jsonDecode(payload) as Map<String, dynamic>;
      CallNotificationManager.receiveResponse(payload, actionId: actionId);
      final client = data['client'];
      final room = data['room'];
      if (client is int && room is String) {
        IntentManager.notificationRouteListener.value =
            '/client/$client/rooms/${Uri.encodeComponent(room)}';
      }
    } catch (_) {
      // Ignore payloads produced by older app versions.
    }
  }

  final settings = const SettingsInterface();
  final Client client;
  final notificationsPlugin = FlutterLocalNotificationsPlugin();

  late AppLocalizations localizations;

  String get instance => client.clientName;

  String? endpoint;
  Future<void>? _registrationRetry;

  void _runCallback(
    String name,
    Future<void> Function() callback, {
    bool retryRegistration = false,
  }) {
    unawaited(
      callback().catchError((Object error, StackTrace stackTrace) {
        Logs().e('UnifiedPush $name callback failed.', error, stackTrace);
        if (retryRegistration) _scheduleRegistrationRetry();
      }),
    );
  }

  void _scheduleRegistrationRetry() {
    if (_registrationRetry != null) return;
    late final Future<void> retry;
    retry = Future<void>.delayed(const Duration(seconds: 30)).then((_) async {
      if (identical(_registrationRetry, retry)) {
        _registrationRetry = null;
      }
      if (!client.isLogged()) return;
      await _initialize();
    });
    _registrationRetry = retry;
  }

  Future<void> _initialize() async {
    try {
      final locale = WidgetsBinding.instance.platformDispatcher
              .computePlatformResolvedLocale(
            AppLocalizations.supportedLocales,
          ) ??
          const Locale('en');
      localizations = await AppLocalizations.delegate.load(locale);
      await initializeNotificationPlugin(localizations);
      final registered = await UnifiedPush.initialize(
        onNewEndpoint: (endpoint, instance) => _runCallback(
          'new endpoint',
          () => onNewEndpoint(endpoint, instance),
          retryRegistration: true,
        ),
        onRegistrationFailed: onRegistrationFailed,
        onUnregistered: (instance) => _runCallback(
          'unregistered',
          () => onUnregistered(instance),
        ),
        onMessage: (message, instance) => _runCallback(
          'message',
          () => onMessage(message, instance),
        ),
        onTempUnavailable: onTempUnavailable,
        linuxOptions: LinuxOptions(
          dbusName: 'business.braid.polycule',
          storage: UnifiedPushStoragePolycule(),
          background: false,
        ),
      );
      if (registered || await UnifiedPush.tryUseCurrentOrDefaultDistributor()) {
        await register();
      }
    } on UnimplementedError catch (_) {
      // UnifiedPush is not available on this platform.
    } catch (error, stackTrace) {
      Logs().e('UnifiedPush initialization failed.', error, stackTrace);
      _scheduleRegistrationRetry();
    }
  }

  Future<void> onNewEndpoint(PushEndpoint endpoint, String instance) async {
    if (instance != this.instance) {
      return;
    }
    // fallback on https://matrix.gateway.unifiedpush.org/_matrix/push/v1/notify
    final uri = await client.checkPushGateway(endpoint.url);
    final pushKey = endpoint.url;
    final pushId = pushKey.split('/').last;
    final appId = 'business.braid.polycule.${client.deviceID}';

    final pusher = Pusher(
      appId: appId,
      pushkey: pushKey,
      appDisplayName: localizations.appName,
      data: PusherData(
        url: uri,
        // The full push envelope lets incoming m.call.invite events reach the
        // native call surface immediately. With event_id_only the headless
        // isolate must open the database and fetch the event first, which can
        // take tens of seconds after Android has woken the application.
        format: null,
        additionalProperties: {'data_message': pusherDataMessageFormat},
      ),
      deviceDisplayName:
          '${client.deviceName ?? localizations.appName} - $pushId',
      kind: 'http',
      lang: localizations.language.replaceAll('_', '-'),
    );

    await client.postPusher(
      pusher,
    );
    await settings.storePushKey(client.clientName, pushKey);
    this.endpoint = pushKey;
    await _removeStaleDevicePushers(
      appId: appId,
      activePushKey: pushKey,
    );
  }

  Future<void> _removeStaleDevicePushers({
    required String appId,
    required String activePushKey,
  }) async {
    try {
      final pushers = await client.getPushers() ?? const <Pusher>[];
      final stale = staleDevicePushers(
        pushers,
        appId: appId,
        activePushKey: activePushKey,
      );
      for (final pusher in stale) {
        await client.deletePusher(
          PusherId(appId: pusher.appId, pushkey: pusher.pushkey),
        );
      }
      if (stale.isNotEmpty) {
        await PushLogJournal.record(
          'Removed ${stale.length} stale UnifiedPush endpoint(s) for this '
          'device.',
          important: true,
        );
      }
    } catch (error, stackTrace) {
      // A cleanup failure must never undo the newly registered working
      // endpoint. It will be retried on the next endpoint callback.
      await PushLogJournal.record(
        'Unable to remove stale UnifiedPush endpoints.',
        level: Level.warning,
        error: error,
        stackTrace: stackTrace,
      );
    }
  }

  void onRegistrationFailed(FailedReason reason, String instance) {
    if (instance != this.instance) {
      return;
    }
    Logs().w('UnifiedPush registration failed for $instance: $reason');
    if (reason != FailedReason.actionRequired) {
      _scheduleRegistrationRetry();
    }
  }

  void onTempUnavailable(String instance) {
    if (instance != this.instance) return;
    Logs().w('UnifiedPush distributor is temporarily unavailable.');
    _scheduleRegistrationRetry();
  }

  Future<void> unregister() async {
    await UnifiedPush.unregister(instance);
  }

  Future<void> onUnregistered(String instance) async {
    if (instance != this.instance) {
      return;
    }
    final pushKey = await settings.getPushKey(client.clientName);
    if (pushKey == null) {
      return;
    }

    await client.deletePusher(
      PusherId(
        appId: 'business.braid.polycule.${client.deviceID}',
        pushkey: pushKey,
      ),
    );
  }

  Future<void> onMessage(PushMessage message, String instance) async {
    if (instance != client.clientName) {
      return;
    }
    await handlePushNotification(
      client: client,
      l10n: localizations,
      message: message.content,
    );
  }

  Future<void> register() async {
    final permission = kIsWeb || !Platform.isAndroid
        ? true
        : await notificationsPlugin
            .resolvePlatformSpecificImplementation<
                AndroidFlutterLocalNotificationsPlugin>()
            ?.requestNotificationsPermission();
    if (permission == null) {
      return;
    }
    await UnifiedPush.register(instance: instance);
  }
}

List<Pusher> staleDevicePushers(
  Iterable<Pusher> pushers, {
  required String appId,
  required String activePushKey,
}) =>
    pushers
        .where(
          (pusher) => pusher.appId == appId && pusher.pushkey != activePushKey,
        )
        .toList(growable: false);
