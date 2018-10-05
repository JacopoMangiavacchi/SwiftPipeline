import XCTest
@testable import SwiftPipeline

final class SwiftPipelineTests: XCTestCase {
    func testFakePipeline() {
        var pipeline = Pipeline()

        pipeline.append(transformer: FakeMapper(name: "fakeMapper1"))
        pipeline.append(transformer: FakeMapper(name: "fakeMapper2"))
        pipeline.append(transformer: FakeFeaturizer(name: "fakeFeaturizer1"))
        pipeline.append(transformer: FakeFeaturizer(name: "fakeFeaturizer2"))

        var result = pipeline.run(input: .String(value: "Jacopo")).compactMap({ (datatype) -> String? in
            if case .String(let value) = datatype {
                return value
            }
            return nil
        })

        XCTAssertEqual(result.count, 2)
        XCTAssertEqual(result[0], "Jacopo")
        XCTAssertEqual(result[1], "Jacopo")
    }

    static var allTests = [
        ("testFakePipeline", testFakePipeline),
    ]
}
