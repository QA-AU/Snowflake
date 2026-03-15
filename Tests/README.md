# Sharp-I Tests

## Test Suites

| File | What it covers |
|------|----------------|
| `DetectionTests.swift` | ConfidenceTier assignment & boundaries, cosine similarity (identical / orthogonal / opposite / empty / mismatched), BoundingBoxData CGRect conversion, DetectionEngine initial state |
| `PersonSearchTests.swift` | KnownPerson model init & embedding storage, person-mode SearchQuery, DateRange enum values |
| `iCloudAssetTests.swift` | iCloud enable/persist, SearchSession lifecycle, KnownItem model |
| `ThermalAndSettingsTests.swift` | ThermalStateManager init, AppSettings defaults (voice, amber), API key Keychain round-trip + deletion, ConfidenceTier tier/color/readout values |
| `VoiceReadoutTests.swift` | Tier readout eligibility, VoiceReadoutService lifecycle (init, read, stop), amber opt-in path |
| `UITests.swift` | XCUITest: launch, nav title, mode toggle, Scan CTA enabled/disabled, person-mode switch, date chips, settings sheet |

## Running Tests

Press `⌘U` in Xcode to run all targets, or pick a suite in the Test Navigator (`⌘6`).

```bash
# CLI (requires a booted simulator)
xcodebuild test \
  -scheme SharpI \
  -destination 'platform=iOS Simulator,name=iPhone 16' \
  -only-testing SharpITests
```

## Notes

- **`UITests`** requires a simulator or device. Camera permission alert will appear on first run — tap "Allow" or pre-grant in scheme settings.
- **`ThermalAndSettingsTests`** writes a temporary Keychain entry (prefixed `hf_test_`) and deletes it after each test.
- Cosine similarity assertions use `accuracy: 0.001` to handle Float precision.
- `VoiceReadoutTests` verifies no-crash behaviour; `isSpeaking` is not asserted because `AVSpeechSynthesizer` fires asynchronously on device.

## Pending (next sprint)

- [ ] `PersonSearchService` tests (PHPhotoLibrary mocked via `PHImageManagerProtocol`)
- [ ] `ImageResizer` unit tests — 512×512 downsampling accuracy
- [ ] `DetectionEngine` integration test with a stubbed `HuggingFaceService`
- [ ] Snapshot tests for `ResultCard` at all three confidence tiers
- [ ] Performance test: cosine similarity on 512-dim embeddings (CLIP output size)
