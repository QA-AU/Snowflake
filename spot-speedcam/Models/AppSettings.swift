import Foundation
import Security
import Combine

// MARK: - AppSettings

/// Central store for user preferences. Much simpler than Sharp-I's version:
/// no iCloud sync, no SwiftData — speed-cam detection has no history to persist.
///
/// Keychain: HuggingFace API key (for CLIP fallback while CoreML model is absent)
/// UserDefaults: all other runtime prefs
final class AppSettings: ObservableObject {

    // MARK: - Published preferences

    /// Whether audio alert is enabled (if false, haptic only)
    @Published var audioAlertEnabled: Bool {
        didSet { UserDefaults.standard.set(audioAlertEnabled, forKey: Keys.audioAlert) }
    }

    /// Minimum confidence to fire an alert (0.55 – 0.90)
    @Published var confidenceThreshold: Double {
        didSet { UserDefaults.standard.set(confidenceThreshold, forKey: Keys.confidence) }
    }

    /// Frame-skip interval (lower = more sensitive, more CPU)
    @Published var frameSkip: Int {
        didSet { UserDefaults.standard.set(frameSkip, forKey: Keys.frameSkip) }
    }

    /// Cooldown seconds between repeated alerts for the same vehicle
    @Published var cooldownSeconds: Double {
        didSet { UserDefaults.standard.set(cooldownSeconds, forKey: Keys.cooldown) }
    }

    /// Whether the CLIP fallback HUD warning is dismissed
    @Published var clipWarningDismissed: Bool {
        didSet { UserDefaults.standard.set(clipWarningDismissed, forKey: Keys.clipWarning) }
    }

    // MARK: - Init

    init() {
        let d = UserDefaults.standard
        audioAlertEnabled   = d.object(forKey: Keys.audioAlert)   as? Bool   ?? true
        confidenceThreshold = d.object(forKey: Keys.confidence)   as? Double ?? 0.60
        frameSkip           = d.object(forKey: Keys.frameSkip)    as? Int    ?? 5
        cooldownSeconds     = d.object(forKey: Keys.cooldown)     as? Double ?? 8.0
        clipWarningDismissed = d.object(forKey: Keys.clipWarning) as? Bool   ?? false
    }

    // MARK: - Keychain (HuggingFace API key for CLIP fallback)

    private static let keychainAccount = "com.spotspeedcam.hf_api_key"

    func saveHuggingFaceKey(_ key: String) {
        let data = Data(key.utf8)
        let query: [String: Any] = [
            kSecClass as String:            kSecClassGenericPassword,
            kSecAttrAccount as String:      Self.keychainAccount,
            kSecValueData as String:        data,
            kSecAttrAccessible as String:   kSecAttrAccessibleWhenUnlocked
        ]
        SecItemDelete(query as CFDictionary)
        SecItemAdd(query as CFDictionary, nil)
    }

    func deleteHuggingFaceKey() {
        let query: [String: Any] = [
            kSecClass as String:       kSecClassGenericPassword,
            kSecAttrAccount as String: Self.keychainAccount
        ]
        SecItemDelete(query as CFDictionary)
    }

    /// Static accessor used by SpeedCamDetector (which has no reference to AppSettings).
    static func loadHuggingFaceKey() -> String? {
        let query: [String: Any] = [
            kSecClass as String:            kSecClassGenericPassword,
            kSecAttrAccount as String:      keychainAccount,
            kSecReturnData as String:       true,
            kSecMatchLimit as String:       kSecMatchLimitOne
        ]
        var result: AnyObject?
        SecItemCopyMatching(query as CFDictionary, &result)
        guard let data = result as? Data else { return nil }
        return String(data: data, encoding: .utf8)
    }

    // MARK: - UserDefaults keys

    private enum Keys {
        static let audioAlert  = "audioAlertEnabled"
        static let confidence  = "confidenceThreshold"
        static let frameSkip   = "frameSkip"
        static let cooldown    = "cooldownSeconds"
        static let clipWarning = "clipWarningDismissed"
    }
}
