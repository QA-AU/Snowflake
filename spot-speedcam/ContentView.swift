import SwiftUI

/// Root navigation state for Spot-SpeedCam.
/// Unlike Sharp-I's search/camera/results flow, this app has a single
/// operational mode: always scanning. Settings slide in as a sheet.
enum AppScreen {
    case camera      // Live viewfinder + scanning HUD
    case settings    // Preferences sheet
}

struct ContentView: View {
    @State private var screen: AppScreen = .camera
    @EnvironmentObject private var appSettings: AppSettings

    var body: some View {
        ZStack {
            switch screen {
            case .camera:
                CameraView(onOpenSettings: { screen = .settings })
            case .settings:
                // Settings is presented as a sheet over camera so the
                // inference loop keeps running while user adjusts prefs.
                CameraView(onOpenSettings: {})
            }
        }
        .sheet(isPresented: Binding(
            get: { screen == .settings },
            set: { if !$0 { screen = .camera } }
        )) {
            SettingsView()
        }
        .preferredColorScheme(.dark)
    }
}
