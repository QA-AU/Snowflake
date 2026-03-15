import SwiftUI

struct ResultsView: View {

    let capturedImage: UIImage
    let query: SearchQuery
    var onBack: () -> Void
    var onNewSearch: () -> Void

    @EnvironmentObject private var appSettings: AppSettings
    @StateObject private var engine       = DetectionEngine()
    @StateObject private var voiceService = VoiceReadoutService()
    @State private var results: [DetectionResult] = []
    @State private var error: Error?

    private var isCrowded: Bool { results.count > 5 }

    var body: some View {
        VStack(spacing: 0) {
            frozenFrame
            if engine.isRunning {
                ProgressView("Scanning…").tint(.white).padding()
            } else if let error {
                Text(error.localizedDescription).foregroundColor(.red).padding()
            } else {
                resultsList
            }
            VoiceReadoutBar(isPlaying: voiceService.isSpeaking) {
                voiceService.read(results: results, amberEnabled: appSettings.amberReadoutEnabled)
            } onStop: {
                voiceService.stop()
            }
            actionBar
        }
        .background(Color.black.ignoresSafeArea())
        .task { await runSearch() }
        .preferredColorScheme(.dark)
    }

    // MARK: - Frozen Frame + Overlay

    private var frozenFrame: some View {
        ZStack {
            Image(uiImage: capturedImage)
                .resizable()
                .scaledToFit()
                .frame(maxHeight: 360)
            OverlayView(results: results)
                .frame(maxHeight: 360)
        }
    }

    // MARK: - Results List

    private var resultsList: some View {
        ScrollView {
            VStack(spacing: 0) {
                if isCrowded {
                    Text("Crowded scene — showing top \(min(results.count, 5)) results")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .padding(.horizontal)
                        .padding(.top, 8)
                }
                LazyVStack(spacing: 12) {
                    ForEach(results.prefix(isCrowded ? 5 : results.count), id: \.id) { result in
                        ResultCard(result: result)
                    }
                }
                .padding()
            }
        }
    }

    // MARK: - Action Bar

    private var actionBar: some View {
        HStack {
            Button("Back", action: onBack).buttonStyle(.bordered)
            Spacer()
            Button("New Search", action: onNewSearch).buttonStyle(.borderedProminent)
        }
        .padding()
    }

    // MARK: - Search

    private func runSearch() async {
        do {
            results = try await engine.search(query: query, in: capturedImage)
            if appSettings.voiceReadoutEnabled {
                voiceService.read(results: results, amberEnabled: appSettings.amberReadoutEnabled)
            }
        } catch {
            self.error = error
        }
    }
}

// MARK: - OverlayView (UIKit bridge)

struct OverlayView: UIViewRepresentable {
    let results: [DetectionResult]

    func makeUIView(context: Context) -> UIView {
        let view = UIView()
        view.backgroundColor = .clear
        context.coordinator.renderer = OverlayRenderer(hostView: view)
        return view
    }

    func updateUIView(_ uiView: UIView, context: Context) {
        context.coordinator.renderer?.render(results: results)
    }

    func makeCoordinator() -> Coordinator { Coordinator() }

    class Coordinator {
        var renderer: OverlayRenderer?
    }
}

// MARK: - ResultCard

struct ResultCard: View {
    let result: DetectionResult

    private var tierColor: Color {
        switch result.tier {
        case .green:  return Color(hex: "#00C853") ?? .green
        case .yellow: return Color(hex: "#FFD600") ?? .yellow
        case .amber:  return Color(hex: "#FF6D00") ?? .orange
        }
    }

    var body: some View {
        HStack {
            RoundedRectangle(cornerRadius: 3)
                .fill(tierColor)
                .frame(width: 4, height: 44)
            VStack(alignment: .leading, spacing: 2) {
                Text(result.label)
                    .font(.headline)
                    .foregroundColor(.white)
                Text(String(format: "%.0f%% confidence", result.confidence * 100))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(Color.white.opacity(0.07))
        .cornerRadius(10)
    }
}

// MARK: - VoiceReadoutBar

struct VoiceReadoutBar: View {
    let isPlaying: Bool
    let onPlay: () -> Void
    let onStop: () -> Void

    var body: some View {
        HStack {
            Image(systemName: isPlaying ? "speaker.wave.3.fill" : "speaker.fill")
                .foregroundColor(.white)
            Text(isPlaying ? "Reading results…" : "Voice readout")
                .font(.caption)
                .foregroundStyle(.secondary)
            Spacer()
            Button(isPlaying ? "Stop" : "Play", action: isPlaying ? onStop : onPlay)
                .font(.caption.bold())
                .foregroundColor(.accentColor)
        }
        .padding(.horizontal)
        .padding(.vertical, 10)
        .background(Color.white.opacity(0.05))
    }
}

// MARK: - Color hex init (SwiftUI)

extension Color {
    init?(hex: String) {
        guard let uiColor = UIColor(hex: hex) else { return nil }
        self.init(uiColor)
    }
}
