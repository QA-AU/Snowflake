import SwiftUI
import SwiftData

@main
struct SharpIApp: App {
    let container: ModelContainer

    init() {
        do {
            container = try ModelContainer(
                for: SearchQuery.self,
                     DetectionResult.self,
                     KnownItem.self,
                     KnownPerson.self,
                     SearchSession.self
            )
        } catch {
            fatalError("Failed to initialize ModelContainer: \(error)")
        }
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
                .modelContainer(container)
                .preferredColorScheme(.dark)
        }
    }
}
