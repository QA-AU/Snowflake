import SwiftUI
import AVFoundation

@main
struct SpotSpeedCamApp: App {
    @StateObject private var appSettings = AppSettings()
    @StateObject private var alertService = AlertService()
    @StateObject private var thermalManager = ThermalManager()
    @StateObject private var backgroundManager = BackgroundManager()

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(appSettings)
                .environmentObject(alertService)
                .environmentObject(thermalManager)
                .environmentObject(backgroundManager)
                .onReceive(NotificationCenter.default.publisher(for: UIApplication.willResignActiveNotification)) { _ in
                    backgroundManager.appDidBackground()
                }
                .onReceive(NotificationCenter.default.publisher(for: UIApplication.didBecomeActiveNotification)) { _ in
                    backgroundManager.appDidForeground()
                }
        }
    }
}
