import AVFoundation
import UIKit

// MARK: - FrameGrabber
// Captures a single CVPixelBuffer on tap and converts it to UIImage for the HF path.

struct FrameGrabber {

    /// High-level entry: grabs next frame from the camera session.
    static func grab(from session: CameraSession) async throws -> UIImage {
        let pixelBuffer = try await session.grabFrame()
        return try toUIImage(pixelBuffer)
    }

    // MARK: - CVPixelBuffer → UIImage

    static func toUIImage(_ pixelBuffer: CVPixelBuffer) throws -> UIImage {
        let ciImage = CIImage(cvPixelBuffer: pixelBuffer)
        let context = CIContext(options: nil)
        let w = CVPixelBufferGetWidth(pixelBuffer)
        let h = CVPixelBufferGetHeight(pixelBuffer)
        guard let cgImage = context.createCGImage(ciImage, from: CGRect(x: 0, y: 0, width: w, height: h)) else {
            throw FrameGrabberError.conversionFailed
        }
        return UIImage(cgImage: cgImage)
    }

    enum FrameGrabberError: Error {
        case conversionFailed
    }
}
