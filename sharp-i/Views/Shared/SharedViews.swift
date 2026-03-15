import SwiftUI

// MARK: - SettingsView

struct SettingsView: View {
    @EnvironmentObject private var appSettings: AppSettings
    @State private var apiKeyInput = ""
    @State private var showAPIKey  = false

    var body: some View {
        NavigationStack {
            Form {
                Section("Hugging Face API") {
                    HStack {
                        Group {
                            if showAPIKey {
                                TextField("API key", text: $apiKeyInput)
                                    .textInputAutocapitalization(.never)
                                    .autocorrectionDisabled()
                            } else {
                                SecureField("API key", text: $apiKeyInput)
                            }
                        }
                        Button {
                            showAPIKey.toggle()
                        } label: {
                            Image(systemName: showAPIKey ? "eye.slash" : "eye")
                        }
                    }
                    Button("Save") {
                        appSettings.huggingFaceAPIKey = apiKeyInput
                    }
                    .disabled(apiKeyInput.isEmpty)
                }

                Section("Voice Readout") {
                    Toggle("Enable voice readout", isOn: $appSettings.voiceReadoutEnabled)
                    Toggle("Read amber-tier results", isOn: $appSettings.amberReadoutEnabled)
                        .disabled(!appSettings.voiceReadoutEnabled)
                }

                Section("Sync") {
                    Toggle("iCloud sync", isOn: $appSettings.iCloudSyncEnabled)
                }

                Section("Confidence Tiers") {
                    LegendRow(color: .green,  label: "High  — > 85%")
                    LegendRow(color: .yellow, label: "Medium — 60–85%")
                    LegendRow(color: .orange, label: "Low    — 50–60%")
                }
            }
            .navigationTitle("Settings")
            .scrollContentBackground(.hidden)
            .background(Color.black.ignoresSafeArea())
            .preferredColorScheme(.dark)
            .onAppear { apiKeyInput = appSettings.huggingFaceAPIKey }
        }
    }
}

private struct LegendRow: View {
    let color: Color
    let label: String

    var body: some View {
        HStack {
            Circle().fill(color).frame(width: 10, height: 10)
            Text(label).font(.caption)
        }
    }
}

// MARK: - ThermalBanner

struct ThermalBanner: View {
    @StateObject private var thermalManager = ThermalStateManager()

    var body: some View {
        if thermalManager.isCritical {
            banner(icon: "thermometer.high",
                   text: "Device overheating — scanning paused",
                   bg: .red,
                   fg: .white)
        } else if thermalManager.isSerious {
            banner(icon: "thermometer.medium",
                   text: "Device warm — using lower-power model",
                   bg: .orange,
                   fg: .black)
        }
    }

    private func banner(icon: String, text: String, bg: Color, fg: Color) -> some View {
        HStack(spacing: 8) {
            Image(systemName: icon)
            Text(text).font(.caption.bold())
        }
        .foregroundColor(fg)
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
        .frame(maxWidth: .infinity)
        .background(bg)
    }
}

// MARK: - ICloudPromptSheet

struct ICloudPromptSheet: View {
    @Binding var isPresented: Bool
    @EnvironmentObject private var appSettings: AppSettings

    var body: some View {
        NavigationStack {
            VStack(spacing: 24) {
                Image(systemName: "icloud")
                    .font(.system(size: 60))
                    .foregroundColor(.accentColor)

                VStack(spacing: 8) {
                    Text("Sync with iCloud?")
                        .font(.title2.bold())
                    Text("Your search history and known items will sync across your devices.")
                        .font(.body)
                        .multilineTextAlignment(.center)
                        .foregroundStyle(.secondary)
                }

                VStack(spacing: 12) {
                    Button {
                        appSettings.iCloudSyncEnabled = true
                        isPresented = false
                    } label: {
                        Text("Enable iCloud Sync")
                            .frame(maxWidth: .infinity)
                            .padding()
                            .background(Color.accentColor)
                            .foregroundColor(.white)
                            .cornerRadius(12)
                    }

                    Button("Not Now") { isPresented = false }
                        .foregroundStyle(.secondary)
                }
                .padding(.horizontal)
            }
            .padding()
            .background(Color.black.ignoresSafeArea())
        }
        .preferredColorScheme(.dark)
    }
}
