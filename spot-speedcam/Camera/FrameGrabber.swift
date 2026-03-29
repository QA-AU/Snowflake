import CoreVideo
import CoreImage
import UIKit

/// Converts raw CVPixelBuffer frames from CameraSession into UIImage
/// snapshots for display, and prepares CoreML-ready pixel buffers.
///
/// Reused from Sharp-I with additions:
/// - resizedPixelBuffer(to:) for CoreML model input sizing
/// - Static pool for CIContext to avoid repeated GPU context creation
enum FrameGrabber {

    private static let ciContext = CIContext(options: [.useSoftwareRenderer: false])

    // MARK: - CVPixelBuffer → UIImage

    static func uiImage(from pixelBuffer: CVPixelBuffer) -> UIImage? {
        let ciImage = CIImage(cvPixelBuffer: pixelBuffer)
        guard let cgImage = ciContext.createCGImage(ciImage, from: ciImage.extent) else { return nil }
        return UIImage(cgImage: cgImage, scale: 1.0, orientation: .up)
    }

    // MARK: - Resize for CoreML input

    /// Returns a new CVPixelBuffer resized to `size` (default 640×640 for YOLOv8).
    /// The caller owns the returned buffer — it must not be retained beyond the
    /// inference call to avoid memory pressure.
    static func resizedPixelBuffer(
        from pixelBuffer: CVPixelBuffer,
        to size: CGSize = CGSize(width: 640, height: 640)
    ) -> CVPixelBuffer? {
        let ciImage = CIImage(cvPixelBuffer: pixelBuffer)
        let scaleX = size.width  / CGFloat(CVPixelBufferGetWidth(pixelBuffer))
        let scaleY = size.height / CGFloat(CVPixelBufferGetHeight(pixelBuffer))
        let scaled = ciImage.transformed(by: CGAffineTransform(scaleX: scaleX, y: scaleY))

        var output: CVPixelBuffer?
        let attrs: [String: Any] = [
            kCVPixelBufferCGImageCompatibilityKey as String: true,
            kCVPixelBufferCGBitmapContextCompatibilityKey as String: true
        ]
        CVPixelBufferCreate(
            kCFAllocatorDefault,
            Int(size.width),
            Int(size.height),
            kCVPixelFormatType_32BGRA,
            attrs as CFDictionary,
            &output
        )
        guard let out = output else { return nil }
        ciContext.render(scaled, to: out)
        return out
    }

    // MARK: - JPEG snapshot (for CLIP zero-shot fallback)

    /// Encodes the pixel buffer as JPEG data suitable for multipart upload.
    static func jpegData(from pixelBuffer: CVPixelBuffer, quality: CGFloat = 0.75) -> Data? {
        guard let image = uiImage(from: pixelBuffer) else { return nil }
        return image.jpegData(compressionQuality: quality)
    }
}
