//
//  main.swift
//  TransformationPipeline
//
//  Created by Jacopo Mangiavacchi on 10/4/18.
//  Copyright © 2018 JacopoMangia. All rights reserved.
//

import Foundation

// Basic data type supported for Inputs, Outputs, Features and Metadata
public enum DataType {
    case String1D(array: [String])
    case Float1D(array: [Float])
    case Double1D(array: [Double])
    case String2D(array: [[String]])
    case Float2D(array: [[Float]])
    case Double2D(array: [[Double]])
}

// Pipeline interface passed to Transformers for accessing Pipeline input stack and metadata
public protocol PipelineProtocol {
    var inputs: [DataType] { get }
    var metadatas: [String : DataType] { get }
}

// Generic abstract Transormer interface 
public protocol TransformProtocol {
    var name: String { get }
}

// Specific Transform interface for implementing Mapper
public protocol MapperProtocol : TransformProtocol {
    func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addMetadata: (String, DataType) -> Void)
}

// Specific Transform interface for implementing Featurizer
public protocol FeaturizerProtocol : TransformProtocol {
    func transform(pipeline: PipelineProtocol, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void)
}

// Pipeline object to chain and execute several Mappar and Featurizer tranformers
public struct Pipeline : PipelineProtocol {
    public var inputs = [DataType]()
    public var metadatas = [String : DataType]()
    public var features = [DataType]()
    public var transformers = [TransformProtocol]()

    public init() {
    }
    
    public mutating func append(transformer: TransformProtocol) {
        transformers.append(transformer)
    }
    
    public mutating func run(input: DataType) -> [DataType] {
        inputs.append(input)
        for transformer in transformers {
            print("--- executing \(transformer.name) transformer")
            if let transformer = transformer as? MapperProtocol {
                transformer.transform(pipeline: self,
                                      addOutput: { (output) in inputs.append(output) },
                                      addMetadata: { (name, value) in metadatas[name] = value} )
            }
            if let transformer = transformer as? FeaturizerProtocol {
                transformer.transform(pipeline: self,
                                      addFeature: { (feature) in features.append(feature) },
                                      addMetadata: { (name, value) in metadatas[name] = value} )
            }
        }
        
        return features
    }
}


// Fake Mapper
public struct FakeMapper : MapperProtocol {
    public var name: String
    
    public init(name: String) {
        self.name = name
    }

    public func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addOutput(pipeline.inputs.last!)
    }
}


// Fake Featurizer
public struct FakeFeaturizer : FeaturizerProtocol {
    public var name: String
    
    public init(name: String) {
        self.name = name
    }

    public func transform(pipeline: PipelineProtocol, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addFeature(pipeline.inputs.last!)
    }
}
