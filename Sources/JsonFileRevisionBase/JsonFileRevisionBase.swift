//
//  JsonFileRevision.swift
//  JsonFileRevision
//
//  Created by Bernardo Breder on 04/02/17.
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

public let JsonFileRevisionTable = "revision"

public let JsonFileBranchTable = "branch"

public let JsonFileBranchMaster = "master"

open class JsonFileRevisionBase {
    
    public init() {}
    
}

extension JsonFileRevisionBase {
    
    public class func revisionExist(reader: DataStoreReader, id: Int) throws -> JsonFileRevision? {
        return try reader.exist(name: JsonFileRevisionTable, page: revisionPage(id), id: id, decode: JsonFileRevisionDecoder.decode(record:))
    }
    
    public class func revisionGet(reader: DataStoreReader, id: Int) throws -> JsonFileRevision {
        return try reader.get(name: JsonFileRevisionTable, page: revisionPage(id), id: id, decode: JsonFileRevisionDecoder.decode(record:))
    }
    
    public class func revisionInsert(writer: DataStoreWriter, data: JsonFileRevision) throws {
        writer.insert(name: JsonFileRevisionTable, page: revisionPage(data.id), id: data.id, record: try data.encode())
    }

    public class func revisionPage(_ id: Int) -> Int {
        return id / 32
    }
    
}

extension JsonFileRevisionBase {
    
    public class func branchSequence(writer: DataStoreWriter) throws -> Int {
        return try writer.sequence(name: JsonFileBranchTable)
    }
    
    public class func branchInsert(writer: DataStoreWriter, data: JsonFileRevisionBranch) throws {
        writer.insert(name: JsonFileBranchTable, page: branchPage(data.id), id: data.id, record: data.encode())
    }
    
    public class func branchList(reader: DataStoreReader) throws -> [JsonFileRevisionBranch] {
        return try reader.list(name: JsonFileBranchTable, decode: JsonFileRevisionBranch.init(record:))
    }
    
    public class func branchRemove(writer: DataStoreWriter, id: Int) throws {
        writer.delete(name: JsonFileBranchTable, page: branchPage(id), id: id)
    }
    
    public class func branchPage(_ id: Int) -> Int {
        return id / 32
    }
    
}
