// MARK: - Sharp-I Project Scaffold
//
// This file documents the full folder structure of the Sharp-I Xcode project.
// It is not compiled into the app binary — for documentation purposes only.
//
// sharp-i/
// ├── SharpIApp.swift              — @main App entry, ModelContainer init
// ├── ContentView.swift            — Root navigation state machine (search → camera → results)
// ├── scaffold.swift               — This file
// │
// ├── Models/
// │   ├── Models.swift             — SwiftData schema:
// │   │                              SearchQuery, DetectionResult, KnownItem,
// │   │                              KnownPerson, SearchSession,
// │   │                              BoundingBoxData, ConfidenceTier
// │   └── AppSettings.swift        — ObservableObject + Keychain (HF API key)
// │                                  UserDefaults (voice, amber, iCloud prefs)
// │
// ├── Services/
// │   ├── Detection/
// │   │   └── DetectionEngine.swift — Orchestrates search; embedding cache;
// │   │                               filter (≥0.50), sort (desc), label assignment
// │   ├── Cloud/
// │   │   └── HuggingFaceService.swift — Prototype Hugging Face Inference API:
// │   │                                   CLIP text + image embedding
// │   │                                   DETR object detection
// │   │                                   cosine similarity blend (60/40)
// │   │                                   full search pipeline
// │   └── ThermalAndVoice.swift    — ThermalStateManager  (ProcessInfo observer)
// │                                  VoiceReadoutService  (AVSpeechSynthesizer)
// │                                  LiveInferenceEngine  (CoreML stub)
// │
// ├── Camera/
// │   ├── CameraSession.swift      — AVCaptureSession wrapper, permission gating,
// │   │                              async grabFrame() continuation,
// │   │                              CameraPreviewView (UIViewRepresentable)
// │   └── FrameGrabber.swift       — Tap-to-capture: CVPixelBuffer → UIImage
// │
// ├── Overlays/
// │   └── OverlayRenderer.swift    — BoundingBoxLayer   (CALayer subclass)
// │                                  RenderedResult      (value type)
// │                                  OverlayRenderer     (batched CATransaction)
// │                                  CGRect normalised → pixel
// │                                  UIColor hex initialiser
// │
// ├── Protocols/
// │   └── Protocols.swift          — PhotoKitProviding, PHImageManagerProtocol,
// │                                  ProcessInfoProtocol, InferenceEngineProtocol,
// │                                  ModelLoaderProtocol, AVSpeechSynthesizerProtocol
// │
// └── Views/
//     ├── Search/
//     │   ├── SearchView.swift         — Home: mode toggle, query fields,
//     │   │                              date chips, capture source, Scan CTA
//     │   └── SearchComponents.swift   — ObjectQueryField, PersonQueryFields,
//     │                                  DateChipRow, DateChip, DateRange enum
//     ├── Camera/
//     │   └── CameraView.swift         — Full-screen live feed, QueryChip,
//     │                                  ReticleView, WarmingUpIndicator, ScanButton
//     ├── Results/
//     │   └── ResultsView.swift        — Frozen frame + OverlayView (UIKit bridge),
//     │                                  ResultCard, VoiceReadoutBar,
//     │                                  crowded-scene cap (>5 → show top 5)
//     └── Shared/
//         └── SharedViews.swift        — SettingsView, ThermalBanner, ICloudPromptSheet
//
// Tests/
// ├── DetectionTests.swift
// ├── PersonSearchTests.swift
// ├── iCloudAssetTests.swift
// ├── ThermalAndSettingsTests.swift
// ├── VoiceReadoutTests.swift
// ├── UITests.swift
// └── README.md
//
// MARK: - Confidence Tiers
//   Green  > 0.85   #00C853   voice on
//   Yellow 0.60–0.85 #FFD600  voice on
//   Amber  0.50–0.60 #FF6D00  voice off (user opt-in)
//
// MARK: - Pending (next sprint)
//   • PersonSearchService + PhotoKitService
//   • ImageResizer utility (512×512 downsampling)
//   • NSCameraUsageDescription in Info.plist
//   • Bundle .mlpackage files (CLIP, YOLOv8, FaceNet)
//   • TestFlight setup
//   • Trademark clearance: Sharp-I vs Sharp Electronics ⚠️
