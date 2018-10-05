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
    case String(value: String)
    case StringArray(array: [String])
    case Float(value: Float)
    case FloatArray(array: [Float])
    case Double(value: Double)
    case DoubleArray(array: [Double])
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
    
    public func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addOutput(pipeline.inputs.last!)
    }
}


// Fake Featurizer
public struct FakeFeaturizer : FeaturizerProtocol {
    public var name: String
    
    public func transform(pipeline: PipelineProtocol, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addFeature(pipeline.inputs.last!)
    }
}
