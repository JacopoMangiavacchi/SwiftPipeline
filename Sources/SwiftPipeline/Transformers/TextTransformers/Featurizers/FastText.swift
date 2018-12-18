
//  FastText.swift
//  TransformationPipeline
//
//  Created by Jacopo Mangiavacchi on 2018.
//  Copyright © 2018 JacopoMangia. All rights reserved.
//

import Foundation
import SwiftFastText

// FastText Featurizer
public struct FastText : TransformProtocol, Codable {
    //Base Properties
    public let name: DataString
    public let transformerType: TransformType

    //Parameters: NB read only (let)
    private let fastTextModelPath: String

    //Metadata: NB access on write must be protected for concurrency access if this is a .Mapper

    public init(name: String = "FastText", fastTextModelPath: String) {
        self.name = name
        self.transformerType = .Featurizer
        self.fastTextModelPath = fastTextModelPath
    }
    
    public mutating func transform(input: MatrixDataIO, generateMetadata: Bool) throws -> MatrixDataIO {
        guard let fastTextModelUrl = URL(string: fastTextModelPath) else {
            throw TransformerError.MetadataNotValid
        }
            
        let ft = SwiftFastText(withModelUrl: fastTextModelUrl)

        var vectors = MatrixDataIO()
        for doc in input.toMatrixDataString() {
            let m:VectorDataFloat = ft.getSentenceVector(sentence: doc[0])
            vectors.append(m.toVectorDataIO())
        }

        return vectors
    }
}
