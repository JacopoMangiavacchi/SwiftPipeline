import XCTest
@testable import SwiftPipeline

final class SwiftPipelineTests: XCTestCase {
    func testFakePipeline() {
        var pipeline = Pipeline()

        pipeline.append(transformer: FakeMapper(name: "fakeMapper1"))
        pipeline.append(transformer: FakeMapper(name: "fakeMapper2"))
        pipeline.append(transformer: FakeFeaturizer(name: "fakeFeaturizer1"))
        pipeline.append(transformer: FakeFeaturizer(name: "fakeFeaturizer2"))

        let s = DataType.String1D(array: ["Jacopo"])
        var result = pipeline.run(input: s).compactMap({ (datatype) -> [[String]]? in
            if case .String2D(let array) = datatype {
                return array
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
