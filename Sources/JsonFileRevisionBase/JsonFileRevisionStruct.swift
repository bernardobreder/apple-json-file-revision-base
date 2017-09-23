//
//  JsonFileRevision.swift
//  FileStore
//
//  Created by Bernardo Breder on 04/01/17.
//
//

import Foundation

#if SWIFT_PACKAGE
    import Json
    import FileSystem
    import DataStore
    import JsonFileChange
    import DatabaseFileSystem
#endif


public protocol JsonFileRevision {
    
    var id: Int { get }
    
    var type: JsonFileRevisionType { get }
    
    init(record: DataStoreRecord) throws
    
    func encode() throws -> DataStoreRecord
    
}

public protocol JsonFileRevisionCommit {
    
    var branch: String { get }
    
    func apply(writer: DatabaseFileSystemWriter) throws
    
    func revert(writer: DatabaseFileSystemWriter) throws
    
}

public protocol JsonFileRevisionItemBranch {
    
    var branch: String { get }
    
    var branchHash: Int { get }
    
}

public extension JsonFileRevision {
    
    public func commit(branch: String) -> JsonFileRevisionCommit? {
        guard let commit = self as? JsonFileRevisionCommit else { return nil }
        guard commit.branch == branch else { return nil }
        return commit
    }
    
    @discardableResult
    public func commit(branch: String, _ function: (JsonFileRevisionCommit) throws -> Void) rethrows -> Self {
        guard let commit = self as? JsonFileRevisionCommit else { return self }
        guard commit.branch == branch else { return self }
        try function(commit)
        return self
    }
    
    @discardableResult
    public func createBranch(branch: String, _ function: (JsonFileRevisionCreateBranch) throws -> Void) rethrows -> Self {
        guard let createBranch = self as? JsonFileRevisionCreateBranch else { return self }
        guard createBranch.name == branch else { return self }
        try function(createBranch)
        return self
    }
    
    public func createBranch(branch: String) -> Bool {
        guard let createBranch = self as? JsonFileRevisionCreateBranch else { return false }
        return createBranch.name == branch
    }
    
    public func next(reader: DataStoreReader) throws -> JsonFileRevision? {
        return try reader.exist(name: JsonFileRevisionTable, page: JsonFileRevisionBase.revisionPage(id + 1), id: id + 1, decode: JsonFileRevisionDecoder.decode(record:))
    }
    
    public func prev(reader: DataStoreReader) throws -> JsonFileRevision? {
        return try reader.exist(name: JsonFileRevisionTable, page: JsonFileRevisionBase.revisionPage(id - 1), id: id - 1, decode: JsonFileRevisionDecoder.decode(record:))
    }
    
}

public struct JsonFileRevisionBranch {
    
    public let id: Int
    
    public let name: String
    
    public let createdId: Int
    
    public let lastReintegratedId: Int
    
    public init(id: Int, name: String, createdId: Int, lastReintegratedId: Int) {
        self.id = id
        self.name = name
        self.createdId = createdId
        self.lastReintegratedId = lastReintegratedId
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let name = try record.requireString("name")
        let createdId = try record.requireInt("createdId")
        let lastReintegratedId = try record.requireInt("lastReintegratedId")
        self.init(id: id, name: name, createdId: createdId, lastReintegratedId: lastReintegratedId)
    }
    
    public func encode() -> DataStoreRecord {
        return DataStoreRecord(json: Json(["id": id, "name": name, "createdId": createdId, "lastReintegratedId": lastReintegratedId]))
    }
    
}

public enum JsonFileRevisionType: Int {
    
    case commit = 1
    case createTag
    case removeTag
    case renameTag
    case createBranch
    case removeBranch
    case renameBranch
    case reintegrate
    
    public func decode(record: DataStoreRecord) throws -> JsonFileRevision {
        switch self {
        case .createTag: return try JsonFileRevisionCreateTag(record: record)
        case .removeTag: return try JsonFileRevisionRemoveTag(record: record)
        case .renameTag: return try JsonFileRevisionRenameTag(record: record)
        case .createBranch: return try JsonFileRevisionCreateBranch(record: record)
        case .removeBranch: return try JsonFileRevisionRemoveBranch(record: record)
        case .renameBranch: return try JsonFileRevisionRenameBranch(record: record)
        case .commit: return try JsonFileRevisionCommitBranch(record: record)
        case .reintegrate: return try JsonFileRevisionReintegrateBranch(record: record)
        }
    }
    
}

public class JsonFileRevisionDecoder {
    
    public class func decode(record: DataStoreRecord) throws -> JsonFileRevision {
        guard let type = JsonFileRevisionType(rawValue: try record.requireClassId()) else { throw JsonFileRevisionError.classIdUnknown }
        return try type.decode(record: record)
    }
    
}


public struct JsonFileRevisionCreateTag: JsonFileRevision {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.createTag
    
    public let name: String
    
    public init(id: Int, name: String) {
        self.id = id
        self.name = name
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let name = try record.requireString("name")
        self.init(id: id, name: name)
    }
    
    public func encode() throws -> DataStoreRecord {
        return DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "name": name]))
    }
    
}

public struct JsonFileRevisionRemoveTag: JsonFileRevision {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.removeTag
    
    public let name: String
    
    public init(id: Int, name: String) {
        self.id = id
        self.name = name
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let name = try record.requireString("name")
        self.init(id: id, name: name)
    }
    
    
    public func encode() throws -> DataStoreRecord {
        return DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "name": name]))
    }
    
}

public struct JsonFileRevisionRenameTag: JsonFileRevision {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.renameTag
    
    public let from: String
    
    public let to: String
    
    public init(id: Int, from: String, to: String) {
        self.id = id
        self.from = from
        self.to = to
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let from = try record.requireString("from")
        let to = try record.requireString("to")
        self.init(id: id, from: from, to: to)
    }
    
    public func encode() throws -> DataStoreRecord {
        return DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "from": from,
            "to": to]))
    }
    
}

public struct JsonFileRevisionCreateBranch: JsonFileRevision {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.createBranch
    
    public let name: String
    
    public init(id: Int, name: String) {
        self.id = id
        self.name = name
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let name = try record.requireString("name")
        self.init(id: id, name: name)
    }
    
    public func encode() throws -> DataStoreRecord {
        return DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "name": name]))
    }
    
}

public struct JsonFileRevisionRemoveBranch: JsonFileRevision {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.removeBranch
    
    public let name: String
    
    public init(id: Int, name: String) {
        self.id = id
        self.name = name
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let name = try record.requireString("name")
        self.init(id: id, name: name)
    }
    
    public func encode() throws -> DataStoreRecord {
        return DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "name": name]))
    }
    
}

public struct JsonFileRevisionRenameBranch: JsonFileRevision {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.renameBranch
    
    public let from: String
    
    public let to: String
    
    public init(id: Int, from: String, to: String) {
        self.id = id
        self.from = from
        self.to = to
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let from = try record.requireString("from")
        let to = try record.requireString("to")
        self.init(id: id, from: from, to: to)
    }
    
    public func encode() throws -> DataStoreRecord {
        return DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "from": from,
            "to": to]))
    }
    
}

public struct JsonFileRevisionCommitBranch: JsonFileRevision, JsonFileRevisionCommit {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.commit
    
    public let branch: String
    
    public var changes: [JsonFileChangeProtocol]
    
    public init(id: Int, branch: String, changes: [JsonFileChangeProtocol]) {
        self.id = id
        self.branch = branch
        self.changes = changes
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let branch = try record.requireString("branch")
        let changes = try record.requireArray("changes", mapValue: JsonFileChangeDecoder.decode)
        self.init(id: id, branch: branch, changes: changes)
    }
    
    public func apply(writer: DatabaseFileSystemWriter) throws {
        try changes.forEach { c in try c.apply(writer: writer) }
    }
    
    public func revert(writer: DatabaseFileSystemWriter) throws {
        try changes.reversed().forEach { c in try c.revert(writer: writer) }
    }
    
    public func encode() throws -> DataStoreRecord {
        return try DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "branch": branch,
            "changes": changes.map { c in try c.encode().json }]))
    }
    
}

public struct JsonFileRevisionReintegrateBranch: JsonFileRevision, JsonFileRevisionCommit {
    
    public var id: Int
    
    public let type = JsonFileRevisionType.reintegrate
    
    public let from: String
    
    public var changes: [JsonFileChangeProtocol]
    
    public init(id: Int, from: String, changes: [JsonFileChangeProtocol]) {
        self.id = id
        self.from = from
        self.changes = changes
    }
    
    public init(record: DataStoreRecord) throws {
        let id = try record.requireId()
        let from = try record.requireString("from")
        let changes = try record.requireArray("changes", mapValue: JsonFileChangeDecoder.decode)
        self.init(id: id, from: from, changes: changes)
    }
    
    public var branch: String {
        return JsonFileBranchMaster
    }
    
    public func apply(writer: DatabaseFileSystemWriter) throws {
        try changes.forEach { c in try c.apply(writer: writer) }
    }
    
    public func revert(writer: DatabaseFileSystemWriter) throws {
        try changes.reversed().forEach { c in try c.revert(writer: writer) }
    }
    
    public func encode() throws -> DataStoreRecord {
        return try DataStoreRecord(json: Json([
            DataStoreRecord.classid: type.rawValue,
            "id": id,
            "from": from,
            "changes": changes.map { c in try c.encode().json }]))
    }
    
}

public enum JsonFileRevisionError: Error {
    case classIdUnknown
}
