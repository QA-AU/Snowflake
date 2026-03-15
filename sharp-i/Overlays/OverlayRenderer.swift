import UIKit
import QuartzCore

// MARK: - UIColor hex initialiser

extension UIColor {
    convenience init?(hex: String) {
        var s = hex.trimmingCharacters(in: .whitespacesAndNewlines)
        s = s.hasPrefix("#") ? String(s.dropFirst()) : s
        var rgb: UInt64 = 0
        guard Scanner(string: s).scanHexInt64(&rgb) else { return nil }
        self.init(
            red:   CGFloat((rgb & 0xFF0000) >> 16) / 255,
            green: CGFloat((rgb & 0x00FF00) >>  8) / 255,
            blue:  CGFloat( rgb & 0x0000FF        ) / 255,
            alpha: 1
        )
    }
}

// MARK: - CGRect: normalised → pixel

extension CGRect {
    /// Converts a normalised (0…1) bounding box to pixel coordinates.
    func denormalized(in size: CGSize) -> CGRect {
        CGRect(
            x:      origin.x * size.width,
            y:      origin.y * size.height,
            width:  width    * size.width,
            height: height   * size.height
        )
    }
}

// MARK: - BoundingBoxLayer

final class BoundingBoxLayer: CALayer {

    private let borderLayer = CALayer()
    private let labelLayer  = CATextLayer()

    override init() {
        super.init()
        setup()
    }

    required init?(coder: NSCoder) {
        super.init(coder: coder)
        setup()
    }

    private func setup() {
        borderLayer.borderWidth  = 2
        borderLayer.cornerRadius = 4
        addSublayer(borderLayer)

        labelLayer.fontSize          = 12
        labelLayer.foregroundColor   = UIColor.white.cgColor
        labelLayer.backgroundColor   = UIColor.black.withAlphaComponent(0.6).cgColor
        labelLayer.cornerRadius      = 3
        labelLayer.contentsScale     = UIScreen.main.scale
        labelLayer.alignmentMode     = .center
        addSublayer(labelLayer)
    }

    func configure(with result: RenderedResult, in containerSize: CGSize) {
        let pixelRect = result.normalizedBox.denormalized(in: containerSize)
        frame = pixelRect

        let color = UIColor(hex: result.tier.color) ?? .white
        borderLayer.borderColor = color.cgColor
        borderLayer.frame       = bounds

        let labelText = "\(result.label) \(Int(result.confidence * 100))%"
        labelLayer.string = labelText
        let labelWidth    = min(pixelRect.width, 120)
        labelLayer.frame  = CGRect(x: 0, y: -20, width: labelWidth, height: 18)
    }
}

// MARK: - RenderedResult

struct RenderedResult {
    let label:         String
    let confidence:    Float
    let tier:          ConfidenceTier
    let normalizedBox: CGRect

    init(from result: DetectionResult) {
        label         = result.label
        confidence    = result.confidence
        tier          = result.tier
        normalizedBox = result.boundingBox.cgRect
    }
}

// MARK: - OverlayRenderer

final class OverlayRenderer {

    private var activeLayers: [BoundingBoxLayer] = []
    private weak var hostView: UIView?

    init(hostView: UIView) {
        self.hostView = hostView
    }

    func render(results: [DetectionResult]) {
        guard let hostView else { return }
        activeLayers.forEach { $0.removeFromSuperlayer() }
        activeLayers.removeAll()

        let containerSize = hostView.bounds.size

        CATransaction.begin()
        CATransaction.setDisableActions(true)
        for result in results.map(RenderedResult.init) {
            let layer = BoundingBoxLayer()
            layer.configure(with: result, in: containerSize)
            hostView.layer.addSublayer(layer)
            activeLayers.append(layer)
        }
        CATransaction.commit()
    }

    func clear() {
        activeLayers.forEach { $0.removeFromSuperlayer() }
        activeLayers.removeAll()
    }
}
