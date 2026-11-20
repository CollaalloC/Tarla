import Flutter
import Security
import UIKit

@main
@objc class AppDelegate: FlutterAppDelegate {
  override func application(
    _ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
  ) -> Bool {
    if let controller = window?.rootViewController as? FlutterViewController {
      FlutterMethodChannel(name: "dev.tarla/paired-profiles", binaryMessenger: controller.binaryMessenger)
        .setMethodCallHandler(handleProfiles)
    }
    GeneratedPluginRegistrant.register(with: self)
    return super.application(application, didFinishLaunchingWithOptions: launchOptions)
  }

  private func handleProfiles(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: "dev.tarla.paired-profiles",
      kSecAttrAccount as String: "v1",
      kSecAttrSynchronizable as String: false,
    ]
    func fail(_ status: OSStatus) {
      result(FlutterError(code: "profile_storage", message: "Device profile storage is unavailable", details: status))
    }
    switch call.method {
    case "read":
      var lookup = query
      lookup[kSecReturnData as String] = true
      lookup[kSecMatchLimit as String] = kSecMatchLimitOne
      var item: CFTypeRef?
      let status = SecItemCopyMatching(lookup as CFDictionary, &item)
      if status == errSecItemNotFound { result(nil); return }
      guard status == errSecSuccess, let data = item as? Data,
            let value = String(data: data, encoding: .utf8) else { fail(status); return }
      result(value)
    case "write":
      guard let value = call.arguments as? String else { fail(errSecParam); return }
      let attrs: [String: Any] = [
        kSecValueData as String: Data(value.utf8),
        kSecAttrAccessible as String: kSecAttrAccessibleWhenUnlockedThisDeviceOnly,
      ]
      var status = SecItemUpdate(query as CFDictionary, attrs as CFDictionary)
      if status == errSecItemNotFound {
        let item = query.merging(attrs) { _, new in new }
        status = SecItemAdd(item as CFDictionary, nil)
      }
      guard status == errSecSuccess else { fail(status); return }
      result(nil)
    case "clear":
      let status = SecItemDelete(query as CFDictionary)
      guard status == errSecSuccess || status == errSecItemNotFound else { fail(status); return }
      result(nil)
    default:
      result(FlutterMethodNotImplemented)
    }
  }
}
