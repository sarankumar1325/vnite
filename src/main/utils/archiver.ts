import fse from 'fs-extra'
import archiver from 'archiver'
import path from 'path'
import yauzl from 'yauzl'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import log from 'electron-log/main'

interface ZipOptions {
  // Compression level 1-9, default 9 (best compression)
  compressionLevel?: number
  // If or not the root folder is included, default is false
  includeRoot?: boolean
  // Excluded file or folder patterns
  exclude?: string[]
}

export async function zipFolder(
  sourcePath: string,
  targetDir: string,
  zipName: string,
  options: ZipOptions = {}
): Promise<string> {
  try {
    // Normalization path
    const normalizedSourcePath = path.resolve(sourcePath)
    const normalizedTargetDir = path.resolve(targetDir)

    // Check if the source folder exists
    const sourceExists = await fse.pathExists(normalizedSourcePath)
    if (!sourceExists) {
      throw new Error(`Source folder not found: ${normalizedSourcePath}`)
    }

    // Ensure that the target directory exists
    await fse.ensureDir(normalizedTargetDir)

    // Setting the default options
    const { compressionLevel = 9, includeRoot = false, exclude = [] } = options

    // Build the output file path
    const zipFileName = zipName.endsWith('.zip') ? zipName : `${zipName}.zip`
    const outputPath = path.join(normalizedTargetDir, zipFileName)

    // Creating Compression
    const archive = archiver('zip', {
      zlib: { level: compressionLevel }
    })

    // Creating a Write Stream
    const output = fse.createWriteStream(outputPath)

    // Connect the output stream to the compressor
    archive.pipe(output)

    // Get source folder name
    const folderName = path.basename(normalizedSourcePath)

    // Add files to zip
    if (includeRoot) {
      // Include root folder
      archive.directory(normalizedSourcePath, folderName, (entry) => {
        // Check if the file should be excluded
        if (
          exclude.some(
            (pattern) => entry.name.includes(pattern) || entry.name.match(new RegExp(pattern))
          )
        ) {
          return false
        }
        return entry
      })
    } else {
      // Root folder not included
      archive.directory(normalizedSourcePath, false, (entry) => {
        if (
          exclude.some(
            (pattern) => entry.name.includes(pattern) || entry.name.match(new RegExp(pattern))
          )
        ) {
          return false
        }
        return entry
      })
    }

    // Waiting for compression to complete
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve)
      archive.on('error', reject)
      archive.finalize()
    })

    return outputPath
  } catch (error) {
    log.error(`[Utils] Failed to zip folder ${sourcePath}`, error)
    throw error
  }
}

interface UnzipOptions {
  // Whether to overwrite existing files, default is true.
  overwrite?: boolean
}

export async function unzipFile(
  zipPath: string,
  targetDir: string,
  options: UnzipOptions = {}
): Promise<string[]> {
  try {
    const { overwrite = true } = options
    const normalizedTargetDir = path.resolve(targetDir)
    await fse.ensureDir(normalizedTargetDir)

    // Stream entries one by one so archives larger than 2 GiB (Node's Buffer limit) can be extracted
    const zip = await new Promise<yauzl.ZipFile>((resolve, reject) => {
      yauzl.open(zipPath, { lazyEntries: true, autoClose: true }, (err, zipFile) => {
        if (err || !zipFile) reject(err ?? new Error(`Failed to open zip file: ${zipPath}`))
        else resolve(zipFile)
      })
    })

    const extractedFiles: string[] = []

    await new Promise<void>((resolve, reject) => {
      const fail = (error: Error): void => {
        zip.close()
        reject(error)
      }

      const extractEntry = async (entry: yauzl.Entry): Promise<void> => {
        const filePath = path.resolve(normalizedTargetDir, entry.fileName)
        if (
          filePath !== normalizedTargetDir &&
          !filePath.startsWith(normalizedTargetDir + path.sep)
        ) {
          throw new Error(`Zip entry is outside the target directory: ${entry.fileName}`)
        }

        if (/\/$/.test(entry.fileName)) {
          await fse.ensureDir(filePath)
          return
        }

        if (!overwrite && (await fse.pathExists(filePath))) {
          return
        }

        await fse.ensureDir(path.dirname(filePath))
        const readStream = await new Promise<Readable>((resolveStream, rejectStream) => {
          zip.openReadStream(entry, (err, stream) => {
            if (err || !stream) rejectStream(err ?? new Error(`Failed to read ${entry.fileName}`))
            else resolveStream(stream)
          })
        })
        await pipeline(readStream, fse.createWriteStream(filePath))
        extractedFiles.push(filePath)
      }

      zip.on('entry', (entry: yauzl.Entry) => {
        extractEntry(entry)
          .then(() => zip.readEntry())
          .catch(fail)
      })
      zip.on('end', resolve)
      zip.on('error', fail)
      zip.readEntry()
    })

    return extractedFiles
  } catch (error) {
    log.error(`[Utils] Failed to unzip file ${zipPath}`, error)
    throw error
  }
}
