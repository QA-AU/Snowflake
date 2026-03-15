import Foundation
import Security
import Combine

final class AppSettings: ObservableObject {

    // MARK: - Published

    @Published var huggingFaceAPIKey: String = "" {
        didSet { saveToKeychain(key: KeychainKey.huggingFaceAPIKey, value: huggingFaceAPIKey) }
    }

    @Published var voiceReadoutEnabled: Bool = true {
        didSet { UserDefaults.standard.set(voiceReadoutEnabled, forKey: "voiceReadoutEnabled") }
    }

    @Published var amberReadoutEnabled: Bool = false {
        didSet { UserDefaults.standard.set(amberReadoutEnabled, forKey: "amberReadoutEnabled") }
    }

    @Published var iCloudSyncEnabled: Bool = false {
        didSet { UserDefaults.standard.set(iCloudSyncEnabled, forKey: "iCloudSyncEnabled") }
    }

    // MARK: - Init

    init() { loadSettings() }

    // MARK: - Keychain

    private enum KeychainKey {
        static let huggingFaceAPIKey = "com.sharpi.hf_api_key"
    }

    private func saveToKeychain(key: String, value: String) {
        let query: [String: Any] = [
            kSecClass as String:       kSecClassGenericPassword,
            kSecAttrAccount as String: key
        ]
        SecItemDelete(query as CFDictionary)
        guard !value.isEmpty else { return }
        var addQuery = query
        addQuery[kSecValueData as String]       = value.data(using: .utf8) ?? Data()
        addQuery[kSecAttrAccessible as String]  = kSecAttrAccessibleWhenUnlocked
        SecItemAdd(addQuery as CFDictionary, nil)
    }

    private func loadFromKeychain(key: String) -> String {
        let query: [String: Any] = [
            kSecClass as String:       kSecClassGenericPassword,
            kSecAttrAccount as String: key,
            kSecReturnData as String:  true,
            kSecMatchLimit as String:  kSecMatchLimitOne
        ]
        var result: AnyObject?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
              let data = result as? Data,
              let value = String(data: data, encoding: .utf8) else { return "" }
        return value
    }

    // MARK: - Load

    private func loadSettings() {
        huggingFaceAPIKey   = loadFromKeychain(key: KeychainKey.huggingFaceAPIKey)
        voiceReadoutEnabled = UserDefaults.standard.object(forKey: "voiceReadoutEnabled") as? Bool ?? true
        amberReadoutEnabled = UserDefaults.standard.bool(forKey: "amberReadoutEnabled")
        iCloudSyncEnabled   = UserDefaults.standard.bool(forKey: "iCloudSyncEnabled")
    }
}
