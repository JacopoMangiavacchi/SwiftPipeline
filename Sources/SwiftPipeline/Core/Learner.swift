//
//  Learner.swift
//  TransformationPipeline
//
//  Created by Jacopo Mangiavacchi on 2018.
//  Copyright © 2018 JacopoMangia. All rights reserved.
//

import Foundation

// Learner basic info used for Learner pipeline persistence
public struct LearnerInfo : Codable {
    public let name: DataString
    public let type: DataString
    public let multiClass: Bool
    public let multiLabel: Bool
    
    public init(name: DataString, type: Any, multiClass: Bool, multiLabel: Bool) {
        self.name = name
        self.type = "\(type)"
        self.multiClass = multiClass
        self.multiLabel = multiLabel
    }
}

// Learner Train Result info
public struct TrainResult : Codable {
    public let f1: Float
    public let microAvgAccuracy: Float
    public let macroAvgAccuracy: Float
    public let precision: [Float]
    public let recall: [Float]

    public init() {
        f1 = 0.0
        microAvgAccuracy = 0.0
        macroAvgAccuracy = 0.0
        precision = [Float]()
        recall = [Float]()
    }

    public init(f1: Float, microAvgAccuracy: Float, macroAvgAccuracy: Float, precision: [Float], recall: [Float]) {
        self.f1 = f1
        self.microAvgAccuracy = microAvgAccuracy
        self.macroAvgAccuracy = macroAvgAccuracy
        self.precision = precision
        self.recall = recall
    }
}

// Learner Predict Result info
public struct PredictResult : Codable {
    public let labels: [[Int]]
    public let confidences: [[Float]]

    public init() {
        labels = [[Int]]()
        confidences = [[Float]]()
    }

    public init(labels: [[Int]], confidences: [[Float]]) {
        self.labels = labels
        self.confidences = confidences
    }
}

// Learner interface for generic Classifier
public protocol LearnerProtocol {
    var info: LearnerInfo { get }
    
    init(name: DataString)

    func train(trainFeatures: MatrixDataFloat, testFeatures: MatrixDataFloat, trainLabels: [[Int]], testLabels: [[Int]]) throws -> (trainResult: TrainResult, modelData: Data)
    func predict(modelData: Data, features: MatrixDataFloat) throws -> PredictResult
}
