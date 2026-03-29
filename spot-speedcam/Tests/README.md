# Spot-SpeedCam Test Suite

## Unit Tests

| File | Covers |
|---|---|
| `DetectionTests.swift` | SpeedCamDetection model, isHighConfidence boundary, cosine similarity, IoU / NMS |
| `InferenceLoopTests.swift` | State machine (idle→scanning→detected), double-start guard, detection→alert wiring |
| `AlertTests.swift` | Alert phrase ("mobile speed camera"), de-bounce, stop, isAlerting flag |
| `AppSettingsTests.swift` | UserDefaults defaults + persistence, Keychain save/load/delete |

## UI Tests

| File | Covers |
|---|---|
| `UITests.swift` | App launch, scan toggle (start↔stop), settings sheet open/close, slider + toggle presence |

## Running

```bash
# Unit tests (no device needed)
xcodebuild test -scheme SpotSpeedCam -destination 'platform=iOS Simulator,name=iPhone 15'

# UI tests (simulator or device)
xcodebuild test -scheme SpotSpeedCamUITests -destination 'platform=iOS Simulator,name=iPhone 15'
```

## Notes

- Camera permission must be pre-granted for UI tests (pass `-GrantCameraPermission` launch arg and handle in `AppDelegate`).
- `InferenceLoopTests` uses `StubDetector` and `StubAlertService` to avoid real CoreML / audio hardware.
- Thermal tests are structural only (can't fake `ProcessInfo.thermalState` in unit tests — run on device).
