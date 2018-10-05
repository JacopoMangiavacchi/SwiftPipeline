//
//  main.swift
//  TransformationPipeline
//
//  Created by Jacopo Mangiavacchi on 10/4/18.
//  Copyright © 2018 JacopoMangia. All rights reserved.
//

import Foundation

// Basic data type supported for Inputs, Outputs, Features and Metadata
enum DataType {
    case String(value: String)
    case StringArray(array: [String])
    case Float(value: Float)
    case FloatArray(array: [Float])
    case Double(value: Double)
    case DoubleArray(array: [Double])
}

// Pipeline interface passed to Transformers for accessing Pipeline input stack and metadata
protocol PipelineProtocol {
    var inputs: [DataType] { get }
    var metadatas: [String : DataType] { get }
}

// Transormer interface for implementing Mapper and Featurizer
protocol TransformProtocol {
    var name: String { get }

    func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void)
}

// Pipeline object to chain and execute several Mappar and Featurizer tranformers
struct Pipeline : PipelineProtocol {
    var inputs = [DataType]()
    var metadatas = [String : DataType]()
    var features = [DataType]()
    var transformers = [TransformProtocol]()

    init() {
    }
    
    mutating func append(transformer: TransformProtocol) {
        transformers.append(transformer)
    }
    
    mutating func run(input: DataType) -> [DataType] {
        inputs.append(input)
        for transformer in transformers {
            print("--- executing \(transformer.name) transformer")
            transformer.transform(pipeline: self,
                                  addOutput: { (output) in inputs.append(output) },
                                  addFeature: { (feature) in features.append(feature) },
                                  addMetadata: { (name, value) in metadatas[name] = value} )
        }
        
        return features
    }
}


// Fake Mapper
struct FakeMapper : TransformProtocol {
    var name: String
    
    func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addOutput(pipeline.inputs.last!)
    }
}


// Fake Featurizer
struct FakeFeaturizer : TransformProtocol {
    var name: String
    
    func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addFeature(pipeline.inputs.last!)
    }
}

