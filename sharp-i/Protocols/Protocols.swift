import Foundation
import AVFoundation
import Photos
import UIKit

// MARK: - PhotoKitProviding

protocol PhotoKitProviding {
    func fetchAssets(options: PHFetchOptions?) -> PHFetchResult<PHAsset>
}

// MARK: - PHImageManagerProtocol

protocol PHImageManagerProtocol {
    func requestImage(
        for asset: PHAsset,
        targetSize: CGSize,
        contentMode: PHImageContentMode,
        options: PHImageRequestOptions?,
        resultHandler: @escaping (UIImage?, [AnyHashable: Any]?) -> Void
    ) -> PHImageRequestID
}

extension PHImageManager: PHImageManagerProtocol {}

// MARK: - ProcessInfoProtocol

protocol ProcessInfoProtocol {
    var thermalState: ProcessInfo.ThermalState { get }
}

extension ProcessInfo: ProcessInfoProtocol {}

// MARK: - InferenceEngineProtocol

protocol InferenceEngineProtocol {
    func search(query: SearchQuery, in image: UIImage) async throws -> [DetectionResult]
    var isRunning: Bool { get }
}

// Note: DetectionEngine conforms via its existing implementation.

// MARK: - ModelLoaderProtocol

protocol ModelLoaderProtocol {
    func load() async throws
    var isLoaded: Bool { get }
}

// MARK: - AVSpeechSynthesizerProtocol

protocol AVSpeechSynthesizerProtocol {
    func speak(_ utterance: AVSpeechUtterance)
    @discardableResult func stopSpeaking(at boundary: AVSpeechBoundary) -> Bool
}

extension AVSpeechSynthesizer: AVSpeechSynthesizerProtocol {}
