//
//  main.swift
//  TransformationPipeline
//
//  Created by Jacopo Mangiavacchi on 10/4/18.
//  Copyright © 2018 JacopoMangia. All rights reserved.
//

import Foundation

// Basic data types supported for Inputs, Outputs, Features and Metadata
public enum DataType : String, Codable {
    case String
    case Float
    case Double
}

public struct DataTypeDimension : Codable {
    public let type: DataType
    public let dimension: Int
}

public struct PipelineData : Codable {
    private let value: Any
    public let typeDimension: DataTypeDimension
    
    public var stringValue: String { return value as! String }
    public var stringVector: [String] { return value as! [String] }
    public var stringMatrix: [[String]] { return value as! [[String]] }
    public var floatValue: Float { return value as! Float }
    public var floatVector: [Float] { return value as! [Float] }
    public var floatMatrix: [[Float]] { return value as! [[Float]] }
    public var doubleValue: Double { return value  as! Double }
    public var doubleVector: [Double] { return value as! [Double] }
    public var doubleMatrix: [[Double]] { return value as! [[Double]] }

    public init(_ value: String) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .String, dimension: 0)
    }

    public init(_ value: [String]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .String, dimension: 1)
    }
    
    public init(_ value: [[String]]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .String, dimension: 2)
    }

    public init(_ value: Float) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Float, dimension: 0)
    }
    
    public init(_ value: [Float]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Float, dimension: 1)
    }
    
    public init(_ value: [[Float]]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Float, dimension: 2)
    }

    public init(_ value: Double) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Double, dimension: 0)
    }
    
    public init(_ value: [Double]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Double, dimension: 1)
    }
    
    public init(_ value: [[Double]]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Double, dimension: 2)
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.typeDimension = try values.decode(DataTypeDimension.self, forKey: .typeDimension)
        switch typeDimension.type {
        case .String:
            switch typeDimension.dimension {
            case 0:
                self.value = try values.decode(String.self, forKey: .value)
            case 1:
                self.value = try values.decode([String].self, forKey: .value)
            case 2:
                self.value = try values.decode([[String]].self, forKey: .value)
            default:
                self.value = "ERROR" //TODO
                break
            }
        case .Float:
            switch typeDimension.dimension {
            case 0:
                self.value = try values.decode(Float.self, forKey: .value)
            case 1:
                self.value = try values.decode([Float].self, forKey: .value)
            case 2:
                self.value = try values.decode([[Float]].self, forKey: .value)
            default:
                self.value = "ERROR" //TODO
                break
            }
        case .Double:
            switch typeDimension.dimension {
            case 0:
                self.value = try values.decode(Double.self, forKey: .value)
            case 1:
                self.value = try values.decode([Double].self, forKey: .value)
            case 2:
                self.value = try values.decode([[Double]].self, forKey: .value)
            default:
                self.value = "ERROR" //TODO
                break
            }
        }
    }

    enum CodingKeys: String, CodingKey {
        case typeDimension
        case value
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(typeDimension, forKey: CodingKeys.typeDimension)
        switch typeDimension.type {
        case .String:
            switch typeDimension.dimension {
            case 0:
                try container.encode(value as! String, forKey: CodingKeys.value)
            case 1:
                try container.encode(value as! [String], forKey: CodingKeys.value)
            case 2:
                try container.encode(value as! [[String]], forKey: CodingKeys.value)
            default:
                break
            }
        case .Float:
            switch typeDimension.dimension {
            case 0:
                try container.encode(value as! Float, forKey: CodingKeys.value)
            case 1:
                try container.encode(value as! [Float], forKey: CodingKeys.value)
            case 2:
                try container.encode(value as! [[Float]], forKey: CodingKeys.value)
            default:
                break
            }
        case .Double:
            switch typeDimension.dimension {
            case 0:
                try container.encode(value as! Double, forKey: CodingKeys.value)
            case 1:
                try container.encode(value as! [Double], forKey: CodingKeys.value)
            case 2:
                try container.encode(value as! [[Double]], forKey: CodingKeys.value)
            default:
                break
            }
        }
    }
}

// Transormer basic info used for Pipeline persistence
public struct TransformInfo : Codable {
    public let name: String
    public let type: String
    public let metadatas: [String]
    
    public init(name: String, type: Any, metadatas: [String] = [String]()) {
        self.name = name
        self.metadatas = metadatas
        self.type = "\(type)"
    }
}

// Generic abstract Transormer interface
public protocol TransformProtocol {
    var info: TransformInfo { get }
    
    init(name: String, metadatas: [String])
}

// Specific Transform interface for implementing Mapper
public protocol MapperProtocol : TransformProtocol {
    func transform(pipeline: PipelineProtocol, addOutput: (PipelineData) -> Void, addMetadata: (String, PipelineData) -> Void)
}

// Specific Transform interface for implementing Featurizer
public protocol FeaturizerProtocol : TransformProtocol {
    func transform(pipeline: PipelineProtocol, addFeature: (PipelineData) -> Void, addMetadata: (String, PipelineData) -> Void)
}

// Pipeline interface passed to Transformers for accessing Pipeline input stack and metadata
public protocol PipelineProtocol {
    var inputs: [PipelineData] { get }
    var metadatas: [String : PipelineData] { get }
}

// Pipeline object to chain and execute several Mappar and Featurizer tranformers
public struct Pipeline : PipelineProtocol, Codable {
    public var inputs = [PipelineData]()
    public var metadatas = [String : PipelineData]()
    public var features = [PipelineData]()
    public var transformers = [TransformProtocol]()
    
    enum CodingKeys: String, CodingKey {
        case metadatas
        case transformers
    }
    
    public init() {
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.metadatas = try values.decode([String : PipelineData].self, forKey: .metadatas)

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
    
    public mutating func run(input: PipelineData) {
        inputs.append(input)
        
        //TODO: Execute MapperProtocols sequentially BUT FeaturizerProtocols in parallel
        
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
    }
}

// Fake Mapper
public struct FakeMapper : MapperProtocol {
    public var info: TransformInfo

    public init(name: String, metadatas: [String]) {
        self.info = TransformInfo(name: name, type: type(of: self), metadatas: metadatas)
    }

    public init(name: String = "FakeMapper") {
        self.info = TransformInfo(name: name, type: type(of: self))
    }
    
    public func transform(pipeline: PipelineProtocol, addOutput: (PipelineData) -> Void, addMetadata: (String, PipelineData) -> Void) {
        addOutput(pipeline.inputs.last!)
    }
}


// Fake Featurizer
public struct FakeFeaturizer : FeaturizerProtocol {
    public var info: TransformInfo

    public init(name: String, metadatas: [String]) {
        self.info = TransformInfo(name: name, type: type(of: self), metadatas: metadatas)

    }
    
    public init(name: String = "FakeFeaturizer") {
        self.info = TransformInfo(name: name, type: type(of: self))
    }
    
    public func transform(pipeline: PipelineProtocol, addFeature: (PipelineData) -> Void, addMetadata: (String, PipelineData) -> Void) {
        addFeature(pipeline.inputs.last!)
    }
}
