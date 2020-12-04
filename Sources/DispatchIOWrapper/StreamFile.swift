//
//  File 2.swift
//
//
//  Created by Vitali Kurlovich on 25.11.20.
//

import Foundation

extension StreamFile {
    /*
     convenience init(dispatchIO: DispatchIOWrapper, queue: DispatchQueue,  options: FileOpenOption,  permission: Permission) {
         self.dispatchIO = dispatchIO
         self.permission = permission
         self.options = options
         self.filePath = ""
     }
     */
}

public
final class StreamFile {
    public typealias Error = DispatchIOWrapper.Error
    internal let dispatchIO: DispatchIOWrapper

    public let filePath: String

    public let options: FileOpenOption
    public let permission: Permission

    public
    init(filePath: String, dispatchIO: DispatchIOWrapper, options: FileOpenOption, permission: Permission) {
        self.filePath = filePath
        self.dispatchIO = dispatchIO
        self.options = options
        self.permission = permission
    }

    public
    convenience init?(filePath: String,
          options: FileOpenOption = [.createIfNotExists, .readAndWrite, .append],
          permission: Permission = [.userRead, .userWrite],
          queue: DispatchQueue = DispatchQueue(label: "StreamFile.DispatchQueue.\(UUID().uuidString)"),
          cleanupHandler: @escaping (Result<Void, Error>) -> Void)
    {
        var options = options
        options.insert(.append)
        let io = DispatchIOWrapper(type: .stream, filePath: filePath, options: options, permission: permission, queue: queue, cleanupHandler: cleanupHandler)

        guard let dispatchIO = io else {
            return nil
        }

        self.init(filePath: filePath, dispatchIO: dispatchIO, options: options, permission: permission)
    }

    public
    convenience init?(filePath: String,
          options: FileOpenOption = [.createIfNotExists, .readAndWrite],
          permission: Permission = [.userRead, .userWrite],
          queue: DispatchQueue = DispatchQueue(label: "StreamFile.DispatchQueue.\(UUID().uuidString)"))
    {
        self.init(filePath: filePath, options: options, permission: permission, queue: queue) { _ in
        }
    }

    deinit {
        dispatchIO.close()
    }

    public
    func close() {
        dispatchIO.close()
    }
}

extension StreamFile {
    public
    func read(length: Int = Int(bitPattern: Dispatch.SIZE_MAX),
              progressHandler: @escaping ((Progress) -> Void),
              completion: @escaping (Result<Data, Error>) -> Void)
    {
        dispatchIO.read(offset: 0, length: length, progressHandler: progressHandler, completion: completion)
    }

    public
    func read(length: Int = Int(bitPattern: Dispatch.SIZE_MAX),
              completion: @escaping (Result<Data, Error>) -> Void)
    {
        dispatchIO.read(offset: 0, length: length, progressHandler: { _ in }, completion: completion)
    }
}

extension StreamFile {
    public
    func write(data: Data,
               progressHandler: @escaping ((Progress) -> Void),
               completion: @escaping (Result<Void, Error>) -> Void)
    {
        dispatchIO.write(offset: 0, data: data, progressHandler: progressHandler, completion: completion)
    }

    public
    func write(data: Data,
               completion: @escaping (Result<Void, Error>) -> Void)
    {
        dispatchIO.write(offset: 0, data: data, progressHandler: { _ in }, completion: completion)
    }
}
