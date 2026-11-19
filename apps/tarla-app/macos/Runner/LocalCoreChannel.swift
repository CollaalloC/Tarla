import Cocoa
import Darwin
import FlutterMacOS
import ServiceManagement

/// The standard remote-only app contains no agent plist, so it never registers a service.
final class LocalCoreChannel {
  static let label = "dev.tarla.client.local.tarlad"
  private static let workspace = "desktop-local"
  private let queue = DispatchQueue(label: "dev.tarla.client.local.management")
  private let channel: FlutterMethodChannel

  init(messenger: FlutterBinaryMessenger) {
    channel = FlutterMethodChannel(name: "dev.tarla/local-core", binaryMessenger: messenger)
    channel.setMethodCallHandler { [weak self] call, reply in
      guard let self = self else { return }
      guard #available(macOS 13, *), self.packaged else {
        if call.method == "status" { reply("unavailable") }
        else { reply(FlutterError(code: "unavailable", message: "此版本未提供本机服务", details: nil)) }
        return
      }
      if call.method == "openSettings" {
        SMAppService.openSystemSettingsLoginItems()
        reply(nil)
        return
      }
      self.queue.async {
        do {
          let value = try self.perform(call.method)
          DispatchQueue.main.async { reply(value) }
        } catch {
          DispatchQueue.main.async {
            reply(FlutterError(code: "local_core", message: error.localizedDescription, details: nil))
          }
        }
      }
    }
  }

  private var packaged: Bool {
    Bundle.main.bundleIdentifier == "dev.tarla.client.local" &&
      FileManager.default.fileExists(atPath: Bundle.main.bundleURL
        .appendingPathComponent("Contents/Library/LaunchAgents/\(Self.label).plist").path)
  }

  private var home: URL {
    FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".tarla", isDirectory: true)
  }

  private func failure(_ text: String) -> NSError {
    NSError(domain: "dev.tarla.local-core", code: 1, userInfo: [NSLocalizedDescriptionKey: text])
  }

  @available(macOS 13, *)
  private func state(_ service: SMAppService) -> String {
    switch service.status {
    case .notRegistered: return "disabled"
    case .enabled: return "enabled"
    case .requiresApproval: return "approval"
    case .notFound: return "missing"
    @unknown default: return "unknown"
    }
  }

  @available(macOS 13, *)
  private func perform(_ operation: String) throws -> Any {
    let service = SMAppService.agent(plistName: "\(Self.label).plist")
    switch operation {
    case "status": return state(service)
    case "register":
      if service.status == .notRegistered {
        let cli = Process()
        cli.executableURL = Bundle.main.bundleURL.appendingPathComponent("Contents/Helpers/tarla")
        cli.arguments = ["--home", home.path, "--workspace", Self.workspace, "init", "--node", "mac-desktop"]
        cli.standardOutput = FileHandle.nullDevice
        try cli.run()
        cli.waitUntilExit()
        guard cli.terminationStatus == 0 else { throw failure("初始化本机工作空间失败") }
        do {
          try service.register()
        } catch {
          if service.status != .requiresApproval { throw error }
        }
      }
      return state(service)
    case "unregister":
      if service.status == .enabled || service.status == .requiresApproval { try service.unregister() }
      return state(service)
    case "connection":
      guard service.status == .enabled else { throw failure("请先启用本机服务") }
      let runtime = home.appendingPathComponent("workspaces/\(Self.workspace)/runtime")
      let endpoint = runtime.appendingPathComponent("desktop-endpoint.json")
      let token = runtime.appendingPathComponent("api.token")
      for file in [endpoint, token] {
        let meta = try FileManager.default.attributesOfItem(atPath: file.path)
        guard meta[.type] as? FileAttributeType == .typeRegular,
              (meta[.ownerAccountID] as? NSNumber)?.uint32Value == getuid(),
              let permissions = (meta[.posixPermissions] as? NSNumber)?.uint16Value,
              permissions & 0o077 == 0,
              let size = (meta[.size] as? NSNumber)?.uint64Value, size <= 1024
        else { throw failure("本机服务连接文件权限或大小无效") }
      }
      let dict = try JSONSerialization.jsonObject(with: Data(contentsOf: endpoint)) as? [String: Any]
      guard let port = dict?["port"] as? Int, (1...65535).contains(port) else {
        throw failure("本机服务尚未发布监听端口")
      }
      return ["port": port, "token": try String(contentsOf: token, encoding: .utf8)
        .trimmingCharacters(in: .whitespacesAndNewlines)]
    default: return FlutterMethodNotImplemented
    }
  }
}
