import SwiftUI

// MARK: - ObjectQueryField

struct ObjectQueryField: View {
    @Binding var query: String

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("What are you looking for?")
                .font(.caption)
                .foregroundStyle(.secondary)
            TextField("e.g. red backpack, car keys…", text: $query)
                .textFieldStyle(.roundedBorder)
                .colorScheme(.dark)
        }
    }
}

// MARK: - PersonQueryFields

struct PersonQueryFields: View {
    @Binding var name: String
    @Binding var description: String

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            VStack(alignment: .leading, spacing: 8) {
                Text("Person's name")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextField("Full name", text: $name)
                    .textFieldStyle(.roundedBorder)
                    .colorScheme(.dark)
            }
            VStack(alignment: .leading, spacing: 8) {
                Text("Description (optional)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextField("Wearing, hair colour, approximate age…", text: $description)
                    .textFieldStyle(.roundedBorder)
                    .colorScheme(.dark)
            }
        }
    }
}

// MARK: - DateRange

enum DateRange: String, CaseIterable, Identifiable {
    case today     = "Today"
    case thisWeek  = "This Week"
    case thisMonth = "This Month"
    case allTime   = "All Time"

    var id: String { rawValue }
}

// MARK: - DateChipRow

struct DateChipRow: View {
    @Binding var selected: DateRange?

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 10) {
                ForEach(DateRange.allCases) { range in
                    DateChip(title: range.rawValue, isSelected: selected == range) {
                        selected = (selected == range) ? nil : range
                    }
                }
            }
        }
    }
}

// MARK: - DateChip

struct DateChip: View {
    let title:      String
    let isSelected: Bool
    let action:     () -> Void

    var body: some View {
        Button(action: action) {
            Text(title)
                .font(.caption)
                .padding(.horizontal, 14)
                .padding(.vertical, 7)
                .background(isSelected ? Color.accentColor : Color.gray.opacity(0.25))
                .foregroundColor(isSelected ? .white : .secondary)
                .clipShape(Capsule())
        }
    }
}
