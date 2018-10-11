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

public enum DataDimension : String, Codable {
    case Value
    case Vector
    case Matrix
}

public struct DataTypeDimension : Codable {
    public let type: DataType
    public let dimension: DataDimension
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
        self.typeDimension = DataTypeDimension(type: .String, dimension: .Value)
    }

    public init(_ value: [String]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .String, dimension: .Vector)
    }
    
    public init(_ value: [[String]]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .String, dimension: .Matrix)
    }

    public init(_ value: Float) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Float, dimension: .Value)
    }
    
    public init(_ value: [Float]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Float, dimension: .Vector)
    }
    
    public init(_ value: [[Float]]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Float, dimension: .Matrix)
    }

    public init(_ value: Double) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Double, dimension: .Value)
    }
    
    public init(_ value: [Double]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Double, dimension: .Vector)
    }
    
    public init(_ value: [[Double]]) {
        self.value = value
        self.typeDimension = DataTypeDimension(type: .Double, dimension: .Matrix)
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.typeDimension = try values.decode(DataTypeDimension.self, forKey: .typeDimension)
        switch typeDimension.type {
        case .String:
            switch typeDimension.dimension {
            case .Value:
                self.value = try values.decode(String.self, forKey: .value)
            case .Vector:
                self.value = try values.decode([String].self, forKey: .value)
            case .Matrix:
                self.value = try values.decode([[String]].self, forKey: .value)
            }
        case .Float:
            switch typeDimension.dimension {
            case .Value:
                self.value = try values.decode(Float.self, forKey: .value)
            case .Vector:
                self.value = try values.decode([Float].self, forKey: .value)
            case .Matrix:
                self.value = try values.decode([[Float]].self, forKey: .value)
            }
        case .Double:
            switch typeDimension.dimension {
            case .Value:
                self.value = try values.decode(Double.self, forKey: .value)
            case .Vector:
                self.value = try values.decode([Double].self, forKey: .value)
            case .Matrix:
                self.value = try values.decode([[Double]].self, forKey: .value)
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
            case .Value:
                try container.encode(value as! String, forKey: CodingKeys.value)
            case .Vector:
                try container.encode(value as! [String], forKey: CodingKeys.value)
            case .Matrix:
                try container.encode(value as! [[String]], forKey: CodingKeys.value)
            }
        case .Float:
            switch typeDimension.dimension {
            case .Value:
                try container.encode(value as! Float, forKey: CodingKeys.value)
            case .Vector:
                try container.encode(value as! [Float], forKey: CodingKeys.value)
            case .Matrix:
                try container.encode(value as! [[Float]], forKey: CodingKeys.value)
            }
        case .Double:
            switch typeDimension.dimension {
            case .Value:
                try container.encode(value as! Double, forKey: CodingKeys.value)
            case .Vector:
                try container.encode(value as! [Double], forKey: CodingKeys.value)
            case .Matrix:
                try container.encode(value as! [[Double]], forKey: CodingKeys.value)
            }
        }
    }
}

// Transormer basic info used for Pipeline persistence
public struct TransformInfo : Codable {
    public let name: String
    public let type: String
    public let metadata: [String]
    
    public init(name: String, type: Any, metadata: [String] = [String]()) {
        self.name = name
        self.metadata = metadata
        self.type = "\(type)"
    }
}

// Generic abstract Transormer interface
public protocol TransformProtocol {
    var info: TransformInfo { get }
    
    init(name: String, metadata: [String])
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
    var metadata: [String : PipelineData] { get }
}

// Pipeline object to chain and execute several Mappar and Featurizer tranformers
public struct Pipeline : PipelineProtocol, Codable {
    public var inputs = [PipelineData]()
    public var metadata = [String : PipelineData]()
    public var features = [PipelineData]()
    public var transformers = [TransformProtocol]()
    private var transformersInfo = [TransformInfo]()
    
    enum CodingKeys: String, CodingKey {
        case metadata
        case transformersInfo
    }
    
    public init() {
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.metadata = try values.decode([String : PipelineData].self, forKey: .metadata)
        self.transformersInfo = try values.decode([TransformInfo].self, forKey: .transformersInfo)
    }

    public mutating func injectTransformersFromInfo(transformerMap: [String : TransformProtocol.Type]) {
        for info in transformersInfo {
            if let type: TransformProtocol.Type  = transformerMap[info.type] {
                let transform = type.init(name: info.name, metadata: info.metadata)
                transformers.append(transform)
            }
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(metadata, forKey: CodingKeys.metadata)
        try container.encode(transformersInfo, forKey: CodingKeys.transformersInfo)
    }
    
    public mutating func append(transformer: TransformProtocol) {
        transformers.append(transformer)
        transformersInfo = transformers.map{ $0.info }
    }
    
    public mutating func run(input: PipelineData) {
        inputs.append(input)
        
        //TODO: Execute MapperProtocols sequentially BUT FeaturizerProtocols in parallel
        
        for transformer in transformers {
            print("--- executing \(transformer.info.name) transformer")
            if let transformer = transformer as? MapperProtocol {
                transformer.transform(pipeline: self,
                                      addOutput: { (output) in inputs.append(output) },
                                      addMetadata: { (name, value) in metadata[name] = value} )
            }
            if let transformer = transformer as? FeaturizerProtocol {
                transformer.transform(pipeline: self,
                                      addFeature: { (feature) in features.append(feature) },
                                      addMetadata: { (name, value) in metadata[name] = value} )
            }
        }
    }
}

// Fake Mapper
public struct FakeMapper : MapperProtocol {
    public var info: TransformInfo

    public init(name: String, metadata: [String]) {
        self.info = TransformInfo(name: name, type: type(of: self), metadata: metadata)
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

    public init(name: String, metadata: [String]) {
        self.info = TransformInfo(name: name, type: type(of: self), metadata: metadata)

    }
    
    public init(name: String = "FakeFeaturizer") {
        self.info = TransformInfo(name: name, type: type(of: self))
    }
    
    public func transform(pipeline: PipelineProtocol, addFeature: (PipelineData) -> Void, addMetadata: (String, PipelineData) -> Void) {
        addFeature(pipeline.inputs.last!)
    }
}
