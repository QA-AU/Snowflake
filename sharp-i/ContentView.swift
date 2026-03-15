import SwiftUI

struct ContentView: View {
    @StateObject private var appSettings = AppSettings()
    @State private var appState: AppState = .search
    @State private var capturedImage: UIImage?
    @State private var activeQuery: SearchQuery?

    enum AppState { case search, camera, results }

    var body: some View {
        Group {
            switch appState {
            case .search:
                SearchView { query in
                    activeQuery = query
                    appState = .camera
                }
            case .camera:
                CameraView(
                    query: activeQuery,
                    onFrameCaptured: { image in
                        capturedImage = image
                        appState = .results
                    },
                    onBack: { appState = .search }
                )
            case .results:
                if let image = capturedImage, let query = activeQuery {
                    ResultsView(
                        capturedImage: image,
                        query: query,
                        onBack: { appState = .camera },
                        onNewSearch: {
                            capturedImage = nil
                            activeQuery = nil
                            appState = .search
                        }
                    )
                }
            }
        }
        .environmentObject(appSettings)
    }
}
