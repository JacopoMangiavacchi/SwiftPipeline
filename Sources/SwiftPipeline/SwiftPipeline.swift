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
    case String0D(value: String)
    case Float0D(value: Float)
    case Double0D(value: Double)
    case String1D(array: [String])
    case Float1D(array: [Float])
    case Double1D(array: [Double])
    case String2D(array: [[String]])
    case Float2D(array: [[Float]])
    case Double2D(array: [[Double]])
}

extension DataType : Codable {
    enum Key: CodingKey {
        case rawValue
        case associatedValue
    }
    
    enum CodingError: Error {
        case unknownValue
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: Key.self)
        let rawValue = try container.decode(Int.self, forKey: .rawValue)
        switch rawValue {
        case 0:
            let value = try container.decode(String.self, forKey: .associatedValue)
            self = .String0D(value: value)
        case 1:
            let value = try container.decode(Float.self, forKey: .associatedValue)
            self = .Float0D(value: value)
        case 2:
            let value = try container.decode(Double.self, forKey: .associatedValue)
            self = .Double0D(value: value)
        case 3:
            let array = try container.decode([String].self, forKey: .associatedValue)
            self = .String1D(array: array)
        case 4:
            let array = try container.decode([Float].self, forKey: .associatedValue)
            self = .Float1D(array: array)
        case 5:
            let array = try container.decode([Double].self, forKey: .associatedValue)
            self = .Double1D(array: array)
        case 6:
            let array = try container.decode([[String]].self, forKey: .associatedValue)
            self = .String2D(array: array)
        case 7:
            let array = try container.decode([[Float]].self, forKey: .associatedValue)
            self = .Float2D(array: array)
        case 8:
            let array = try container.decode([[Double]].self, forKey: .associatedValue)
            self = .Double2D(array: array)
        default:
            throw CodingError.unknownValue
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: Key.self)
        switch self {
        case .String0D(let value):
            try container.encode(0, forKey: .rawValue)
            try container.encode(value, forKey: .associatedValue)
        case .Float0D(let value):
            try container.encode(1, forKey: .rawValue)
            try container.encode(value, forKey: .associatedValue)
        case .Double0D(let value):
            try container.encode(2, forKey: .rawValue)
            try container.encode(value, forKey: .associatedValue)
        case .String1D(let array):
            try container.encode(3, forKey: .rawValue)
            try container.encode(array, forKey: .associatedValue)
        case .Float1D(let array):
            try container.encode(4, forKey: .rawValue)
            try container.encode(array, forKey: .associatedValue)
        case .Double1D(let array):
            try container.encode(5, forKey: .rawValue)
            try container.encode(array, forKey: .associatedValue)
        case .String2D(let array):
            try container.encode(6, forKey: .rawValue)
            try container.encode(array, forKey: .associatedValue)
        case .Float2D(let array):
            try container.encode(7, forKey: .rawValue)
            try container.encode(array, forKey: .associatedValue)
        case .Double2D(let array):
            try container.encode(8, forKey: .rawValue)
            try container.encode(array, forKey: .associatedValue)
        }
    }
}

// Transormer basic info used for Pipeline persistence
public struct TransformInfo : Codable {
    let name: String
    let type: String
    let metadatas: [String]?
    
    init(name: String, type: Any /* TransformProtocol.Type */, metadatas: [String]? = nil) {
        self.name = name
        self.metadatas = metadatas
        self.type = "\(type)"
    }
}

// Generic abstract Transormer interface
public protocol TransformProtocol {
    var info: TransformInfo { get }
    
    init(name: String, metadatas: [String]?)
}

// Specific Transform interface for implementing Mapper
public protocol MapperProtocol : TransformProtocol {
    func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addMetadata: (String, DataType) -> Void)
}

// Specific Transform interface for implementing Featurizer
public protocol FeaturizerProtocol : TransformProtocol {
    func transform(pipeline: PipelineProtocol, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void)
}

// Pipeline interface passed to Transformers for accessing Pipeline input stack and metadata
public protocol PipelineProtocol {
    var inputs: [DataType] { get }
    var metadatas: [String : DataType] { get }
}

// Pipeline object to chain and execute several Mappar and Featurizer tranformers
public struct Pipeline : PipelineProtocol, Encodable {
    public var inputs = [DataType]()
    public var metadatas = [String : DataType]()
    public var features = [DataType]()
    public var transformers = [TransformProtocol]()
    
    enum CodingKeys: String, CodingKey {
        case metadatas
        case transformers
    }
    
    public init() {
    }
    
    public init(from decoder: Decoder) throws {
        
        //TODO: using an Depency Injection Dictionaries of TransformProtocol  (ie [info.Type : FakeMapper.self])
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(metadatas, forKey: CodingKeys.metadatas)
        try container.encode(transformers.map( { $0.info } ), forKey: CodingKeys.transformers)
    }
    
    public mutating func append(transformer: TransformProtocol) {
        transformers.append(transformer)
    }
    
    public mutating func run(input: DataType) -> [DataType] {
        inputs.append(input)
        
        //TODO: Execute MapperProtocol sequentially BUT FeaturizerProtocol in parallel
        
        for transformer in transformers {
            print("--- executing \(transformer.info.name) transformer")
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
    public var info: TransformInfo

    public init(name: String, metadatas: [String]?) {
        self.info = TransformInfo(name: name, type: type(of: self), metadatas: metadatas)
    }

    public init(name: String = "FakeMapper") {
        self.info = TransformInfo(name: name, type: type(of: self))
    }
    
    public func transform(pipeline: PipelineProtocol, addOutput: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addOutput(pipeline.inputs.last!)
    }
}


// Fake Featurizer
public struct FakeFeaturizer : FeaturizerProtocol {
    public var info: TransformInfo

    public init(name: String, metadatas: [String]?) {
        self.info = TransformInfo(name: name, type: type(of: self), metadatas: metadatas)
    }
    
    public init(name: String = "FakeFeaturizer") {
        self.info = TransformInfo(name: name, type: type(of: self))
    }
    
    public func transform(pipeline: PipelineProtocol, addFeature: (DataType) -> Void, addMetadata: (String, DataType) -> Void) {
        addFeature(pipeline.inputs.last!)
    }
}

