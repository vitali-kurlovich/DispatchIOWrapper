//
//  File 2.swift
//
//
//  Created by Vitali Kurlovich on 24.11.20.
//

import Dispatch
import Foundation

public
struct FileOpenOption: OptionSet {
    public let rawValue: Int32

    public
    init(rawValue: Int32) {
        self.rawValue = rawValue
    }
    
}

extension FileOpenOption {
    public static let append = FileOpenOption(rawValue: O_APPEND)
    public static let createIfNotExists = FileOpenOption(rawValue: O_CREAT)
    public static let noDelay = FileOpenOption(rawValue: O_NONBLOCK)
    public static let errorIfExists = FileOpenOption(rawValue: O_EXCL)

    public static let readOnly = FileOpenOption(rawValue: O_RDONLY)
    public static let writeOnly = FileOpenOption(rawValue: O_WRONLY)
    public static let readAndWrite = FileOpenOption(rawValue: O_RDWR)
}

public
struct Permission: OptionSet {
    public let rawValue: mode_t

    public
    init(rawValue: mode_t) {
        self.rawValue = rawValue
    }
    
    public static let userReadWriteExec = Permission(rawValue: S_IRWXU)
    public static let userRead = Permission(rawValue: S_IRUSR)
    public static let userWrite = Permission(rawValue: S_IWUSR)
    public static let userExec = Permission(rawValue: S_IXUSR)

    public static let groupReadWriteExec = Permission(rawValue: S_IRWXG)
    public static let groupRead = Permission(rawValue: S_IRGRP)
    public static let groupWrite = Permission(rawValue: S_IWGRP)
    public static let groupExec = Permission(rawValue: S_IXGRP)

    public static let othersReadWriteExec = Permission(rawValue: S_IRWXO)
    public static let othersRead = Permission(rawValue: S_IROTH)
    public static let othersWrite = Permission(rawValue: S_IWOTH)
    public static let othersExec = Permission(rawValue: S_IXOTH)
}

public
final class DispatchIOWrapper {
    public typealias FileError = PosixError
    public typealias StreamType = DispatchIO.StreamType

    internal let dispatchIO: DispatchIO
    internal let dispatchQueue: DispatchQueue
    internal let cleanupHandler: (Result<Void, FileError>) -> Void

    private
    init(dispatchIO: DispatchIO, queue: DispatchQueue, cleanupHandler: @escaping (Result<Void, FileError>) -> Void) {
        self.dispatchIO = dispatchIO
        dispatchQueue = queue
        self.cleanupHandler = cleanupHandler
    }
}

extension DispatchIOWrapper {
    public
    convenience init?(type: StreamType,
          filePath: String,
          options: FileOpenOption = [.createIfNotExists, .readAndWrite],
          permission: Permission = [.userRead, .userWrite],
          queue: DispatchQueue,
          cleanupHandler: @escaping (Result<Void, FileError>) -> Void)
    {
        let oflag = options.rawValue
        let mode = permission.rawValue

        let io = filePath.withCString { (cStr) -> DispatchIO? in
            DispatchIO(type: type, path: cStr, oflag: oflag, mode: mode, queue: queue) { errorCode in
                if let error = FileError(rawValue: errorCode) {
                    cleanupHandler(.failure(error))
                } else {
                    cleanupHandler(.success(()))
                }
            }
        }

        guard let dispatchIO = io else {
            return nil
        }

        self.init(dispatchIO: dispatchIO, queue: queue, cleanupHandler: cleanupHandler)
    }
}

extension DispatchIOWrapper {
    public
    convenience init(type: StreamType, fileDescriptor: Int32, queue: DispatchQueue, cleanupHandler: @escaping (Result<Void, FileError>) -> Void) {
        let dispatchIO = DispatchIO(type: type, fileDescriptor: fileDescriptor, queue: queue) { errorCode in
            if let error = FileError(rawValue: errorCode) {
                cleanupHandler(.failure(error))

            } else {
                cleanupHandler(.success(()))
            }
        }

        self.init(dispatchIO: dispatchIO, queue: queue, cleanupHandler: cleanupHandler)
    }

    public
    convenience init(type: StreamType, io: DispatchIO, queue: DispatchQueue, cleanupHandler: @escaping (Result<Void, FileError>) -> Void) {
        let dispatchIO = DispatchIO(type: type, io: io, queue: queue) { errorCode in

            if let error = FileError(rawValue: errorCode) {
                cleanupHandler(.failure(error))

            } else {
                cleanupHandler(.success(()))
            }
        }

        self.init(dispatchIO: dispatchIO, queue: queue, cleanupHandler: cleanupHandler)
    }
}

extension DispatchIOWrapper {
    public
    func channel(type: StreamType, queue _: DispatchQueue, cleanupHandler: @escaping (Result<Void, FileError>) -> Void) -> DispatchIOWrapper {
        DispatchIOWrapper(type: type, io: dispatchIO, queue: dispatchQueue, cleanupHandler: cleanupHandler)
    }

    public
    func channel(type: StreamType, cleanupHandler: @escaping (Result<Void, FileError>) -> Void) -> DispatchIOWrapper {
        channel(type: type, queue: dispatchQueue, cleanupHandler: cleanupHandler)
    }

    public
    func channel(type: StreamType, queue: DispatchQueue) -> DispatchIOWrapper {
        channel(type: type, queue: queue, cleanupHandler: cleanupHandler)
    }

    public
    func channel(type: StreamType) -> DispatchIOWrapper {
        channel(type: type, queue: dispatchQueue, cleanupHandler: cleanupHandler)
    }
}

extension DispatchIOWrapper {
    public
    func close() {
        dispatchIO.close()
    }

    public
    func read(offset: Int,
              length: Int = Int(bitPattern: Dispatch.SIZE_MAX),
              progressHandler: @escaping ((Progress) -> Void),
              completion: @escaping (Result<Data, FileError>) -> Void)
    {
        let totalUnitCount = Int64(length)
        let progress = Progress(totalUnitCount: totalUnitCount)

        progressHandler(progress)

        var data = Data()
        if length > 0 {
            data.reserveCapacity(length)
        }

        dispatchIO.read(offset: off_t(offset), length: length, queue: dispatchQueue, ioHandler: { done, readedData, errorCode in

            if let error = FileError(rawValue: errorCode) {
                progress.cancel()
                progressHandler(progress)

                completion(.failure(error))
                return
            }

            if done {
                progress.completedUnitCount = totalUnitCount
                progressHandler(progress)

                if let readedData = readedData {
                    data.append(contentsOf: readedData)
                }

                completion(.success(data))

            } else {
                if let readedData = readedData {
                    data.append(contentsOf: readedData)
                    progress.completedUnitCount += Int64(readedData.count)
                }

                progressHandler(progress)
            }

        })
    }

    public
    func write(offset: Int, data: Data,
               progressHandler: @escaping ((Progress) -> Void),
               completion: @escaping (Result<Void, FileError>) -> Void)
    {
        let totalUnitCount = Int64(data.count)
        let progress = Progress(totalUnitCount: totalUnitCount)

        // let ioqueue = dispatchQueue

        let dispatchData = data.withUnsafeBytes { (buffer) -> DispatchData in
            DispatchData(bytes: buffer)
        }

        struct DataHolder {
            var data: Data?
        }

        var holder = DataHolder(data: data)

        progressHandler(progress)

        dispatchIO.write(offset: off_t(offset),
                         data: dispatchData,
                         queue: dispatchQueue,
                         ioHandler: { done, writedData, errorCode in

                             if let error = FileError(rawValue: errorCode) {
                                 progress.cancel()
                                 progressHandler(progress)

                                 completion(.failure(error))
                                 return
                             }

                             if let writedData = writedData {
                                 progress.completedUnitCount += Int64(writedData.count)
                             }

                             if done {
                                 progress.completedUnitCount = totalUnitCount
                                 progressHandler(progress)
                                 completion(.success(()))
                                 holder.data = nil
                             } else {
                                 progressHandler(progress)
                             }
                         })
    }
}
