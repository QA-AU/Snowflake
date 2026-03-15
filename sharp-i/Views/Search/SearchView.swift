import SwiftUI

struct SearchView: View {

    var onScan: (SearchQuery) -> Void

    @State private var mode: SearchQuery.SearchMode = .object
    @State private var objectQuery      = ""
    @State private var personName       = ""
    @State private var personDescription = ""
    @State private var selectedDateRange: DateRange? = nil
    @State private var captureSource: CaptureSource  = .camera
    @State private var showSettings = false

    enum CaptureSource: String, CaseIterable {
        case camera       = "Camera"
        case photoLibrary = "Photos"
    }

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 20) {
                    modeToggle
                    querySection
                    DateChipRow(selected: $selectedDateRange)
                    captureSourcePicker
                    scanButton
                }
                .padding()
            }
            .navigationTitle("Sharp-I")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button { showSettings = true } label: {
                        Image(systemName: "gearshape")
                    }
                }
            }
            .background(Color.black.ignoresSafeArea())
            .sheet(isPresented: $showSettings) { SettingsView() }
        }
        .preferredColorScheme(.dark)
    }

    // MARK: - Sub-views

    private var modeToggle: some View {
        Picker("Mode", selection: $mode) {
            Text("Object").tag(SearchQuery.SearchMode.object)
            Text("Person").tag(SearchQuery.SearchMode.person)
        }
        .pickerStyle(.segmented)
    }

    private var querySection: some View {
        Group {
            if mode == .object {
                ObjectQueryField(query: $objectQuery)
            } else {
                PersonQueryFields(name: $personName, description: $personDescription)
            }
        }
    }

    private var captureSourcePicker: some View {
        Picker("Source", selection: $captureSource) {
            ForEach(CaptureSource.allCases, id: \.self) { Text($0.rawValue).tag($0) }
        }
        .pickerStyle(.segmented)
    }

    private var scanButton: some View {
        Button {
            let text = mode == .object ? objectQuery : personName
            guard !text.isEmpty else { return }
            onScan(SearchQuery(text: text, mode: mode))
        } label: {
            Text("Scan")
                .font(.title2.bold())
                .frame(maxWidth: .infinity)
                .padding()
                .background(Color.accentColor)
                .foregroundColor(.white)
                .cornerRadius(12)
        }
        .disabled(mode == .object ? objectQuery.isEmpty : personName.isEmpty)
    }
}
