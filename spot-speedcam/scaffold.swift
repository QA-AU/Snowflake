// MARK: - Spot-SpeedCam Project Scaffold
//
// spot-speedcam/
// ├── SpotSpeedCamApp.swift               App entry, environment injection
// ├── ContentView.swift                   Root navigation (camera + settings sheet)
// │
// ├── Camera/
// │   ├── CameraSession.swift             AVCaptureSession, continuous rear-cam streaming
// │   └── FrameGrabber.swift              CVPixelBuffer → UIImage + resize for CoreML
// │
// ├── Detection/
// │   ├── SpeedCamDetector.swift          Tier-1: CoreML/YOLOv8 | Tier-2: CLIP zero-shot
// │   └── InferenceLoop.swift             Continuous frame-skip loop, cooldown, state machine
// │
// ├── Alert/
// │   └── AlertService.swift              TTS "mobile speed camera" + tone + haptic
// │
// ├── Background/
// │   └── BackgroundManager.swift         AVAudioSession + BGProcessingTask background keep-alive
// │
// ├── Models/
// │   └── AppSettings.swift               UserDefaults prefs + Keychain HF API key
// │
// ├── Services/
// │   └── ThermalManager.swift            ProcessInfo thermal state → isCritical/isSerious
// │
// ├── Views/
// │   ├── CameraView.swift                Fullscreen viewfinder + HUD + BottomBar
// │   ├── StatusOverlay.swift             Scan state badge, thermal banner, fallback warning
// │   └── SettingsView.swift              Preferences form (confidence, frame rate, cooldown)
// │
// ├── Protocols/
// │   └── Protocols.swift                 InferenceEngineProtocol, AlertEmitterProtocol, etc.
// │
// ├── scaffold.swift                      This file — structure documentation
// │
// └── Tests/
//     ├── DetectionTests.swift            SpeedCamDetection, NMS, cosine similarity
//     ├── InferenceLoopTests.swift        State machine, cooldown, thermal throttle
//     ├── AlertTests.swift                Alert de-bounce, audio session, haptic
//     ├── AppSettingsTests.swift          UserDefaults defaults, Keychain round-trip
//     └── UITests.swift                   XCUITest launch, scan toggle, settings sheet
//
// ─────────────────────────────────────────────────────────────────────────────
// XCODE PROJECT SETUP CHECKLIST
// ─────────────────────────────────────────────────────────────────────────────
// Target: spot-speedcam (iOS 17.0+, iPhone 15+ recommended)
//
// Info.plist keys required:
//   NSCameraUsageDescription   "Spot-SpeedCam needs the rear camera to scan for mobile speed cameras."
//   UIBackgroundModes           audio, processing
//   BGTaskSchedulerPermittedIdentifiers  com.spotspeedcam.restart
//
// Capabilities (Signing & Capabilities tab):
//   ✓ Background Modes → Audio, AirPlay, Picture in Picture
//   ✓ Background Modes → Background processing
//
// Bundle resources to add when ready:
//   SpeedCamDetector.mlpackage    YOLOv8n fine-tuned on mobile speed camera images
//   alert_tone.aiff               Short 440 Hz sine burst (generate with Audacity or bundled)
//
// ─────────────────────────────────────────────────────────────────────────────
// TRAINING DATA ROADMAP (to replace CLIP fallback with CoreML)
// ─────────────────────────────────────────────────────────────────────────────
// Phase 1 — Collect raw images (~500 target)
//   Sources:
//     - YouTube UK dashcam channels (NextBase community, dashcam.co.uk)
//     - Screenshot frame-extraction from known mobile speed cam videos
//     - r/ukdashcam, r/unitedkingdom posts with speed camera sightings
//     - Your own driving footage (most reliable ground truth)
//   Label with: Roboflow, Label Studio, or CVAT
//   Classes needed: [speed_camera]  (single class; car/van context inferred by YOLO)
//
// Phase 2 — Augmentation
//   - Horizontal flip, brightness ±30%, blur (simulates distance)
//   - Rain overlay (UK weather), night + headlight glare
//   - Partial occlusion (other vehicles)
//   - Target: 300 real + 200 augmented = 500 training / 100 val / 50 test
//
// Phase 3 — Train YOLOv8n
//   yolo task=detect mode=train model=yolov8n.pt data=speedcam.yaml epochs=100 imgsz=640
//
// Phase 4 — Export to CoreML
//   yolo export model=runs/detect/train/weights/best.pt format=coreml imgsz=640 nms=True
//   → Rename output to SpeedCamDetector.mlpackage and drag into Xcode
//
// Phase 5 — Validate
//   mAP@0.5 target: ≥ 0.72  (acceptable for alerting; false-positive rate < 5%)
//
// ─────────────────────────────────────────────────────────────────────────────
// KEY ARCHITECTURAL DIFFERENCES FROM SHARP-I
// ─────────────────────────────────────────────────────────────────────────────
//  Sharp-I                           Spot-SpeedCam
//  ─────────────────────────────     ─────────────────────────────────────
//  Front + rear camera               Rear camera only
//  Tap-to-scan (one-shot)            Continuous frame loop (6 infs/sec)
//  HuggingFace primary               CoreML primary, HF CLIP fallback only
//  SwiftData history                 No persistence (no history needed)
//  Multi-result voice readout        Single fixed alert phrase + tone
//  Three confidence tiers            Binary: detected / not detected
//  Open-ended search query           Fixed target: speed camera class
//  Foreground only                   Background audio keep-alive
