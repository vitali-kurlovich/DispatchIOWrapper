//
//  File 2.swift
//
//
//  Created by Vitali Kurlovich on 25.11.20.
//

import Foundation

extension RandomAccessFile {
    public
    func stream(queue: DispatchQueue) -> StreamFile {
        let io = dispatchIO.channel(type: .stream, queue: queue)
        return StreamFile(filePath: filePath, dispatchIO: io, options: options, permission: permission)
    }

    public
    func stream() -> StreamFile {
        let io = dispatchIO.channel(type: .stream)
        return StreamFile(filePath: filePath, dispatchIO: io, options: options, permission: permission)
    }
}

public
final class RandomAccessFile {
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
          options: FileOpenOption = [.createIfNotExists, .readAndWrite],
          permission: Permission = [.userRead, .userWrite],
          queue: DispatchQueue = DispatchQueue(label: "RandomAccessFile.DispatchQueue.\(UUID().uuidString)"),
          cleanupHandler: @escaping (Result<Void, Error>) -> Void)
    {
        let io = DispatchIOWrapper(type: .random, filePath: filePath, options: options, permission: permission, queue: queue, cleanupHandler: cleanupHandler)

        guard let dispatchIO = io else {
            return nil
        }

        self.init(filePath: filePath, dispatchIO: dispatchIO, options: options, permission: permission)
    }

    public
    convenience init?(filePath: String,
          options: FileOpenOption = [.createIfNotExists, .readAndWrite],
          permission: Permission = [.userRead, .userWrite],
          queue: DispatchQueue = DispatchQueue(label: "RandomAccessFile.DispatchQueue.\(UUID().uuidString)"))
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

extension RandomAccessFile {
    public
    func read(offset: Int,
              length: Int,
              progressHandler: @escaping ((Progress) -> Void),
              completion: @escaping (Result<Data, Error>) -> Void)
    {
        dispatchIO.read(offset: offset, length: length, progressHandler: progressHandler, completion: completion)
    }

    public
    func read(offset: Int,
              progressHandler: @escaping ((Progress) -> Void),
              completion: @escaping (Result<Data, Error>) -> Void)
    {
        dispatchIO.read(offset: offset, progressHandler: progressHandler, completion: completion)
    }

    public
    func read(offset: Int,
              length: Int,
              completion: @escaping (Result<Data, Error>) -> Void)
    {
        dispatchIO.read(offset: offset, length: length, progressHandler: { _ in }, completion: completion)
    }

    public
    func read(offset: Int,
              completion: @escaping (Result<Data, Error>) -> Void)
    {
        dispatchIO.read(offset: offset, progressHandler: { _ in }, completion: completion)
    }
}

extension RandomAccessFile {
    public
    func write(offset: Int,
               data: Data,
               progressHandler: @escaping ((Progress) -> Void),
               completion: @escaping (Result<Void, Error>) -> Void)
    {
        dispatchIO.write(offset: offset, data: data, progressHandler: progressHandler, completion: completion)
    }

    public
    func write(offset: Int,
               data: Data,
               completion: @escaping (Result<Void, Error>) -> Void)
    {
        dispatchIO.write(offset: offset, data: data, progressHandler: { _ in }, completion: completion)
    }
}
