/* eslint-disable no-return-assign */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-console */
const fs = require('fs/promises');
const { spawn } = require('child_process');
const { parse: parseFileName, join } = require('path');
const axios = require('axios');
const s3client = require('@aws-sdk/client-s3');

const NOTIFY_API_URL = `${process.env.NOTIFY_API_URL}/assets/notify/file`;

const notify = async ({ filename, size }) => {
    const data = { filename, size };
    try {
        const response = await axios.put(NOTIFY_API_URL, data);
        console.log(`NOTIFY - Status: ${response.status}`);
        console.log('NOTIFY - Body: ', response.data);
    } catch (error) {
        console.error(`NOTIFY - Error: ${error}`);
    }
};

// convert 86 to 01:26:00
const secondsToHms = (d) => {
    const convert = Number(d);
    const h = Math.floor(convert / 3600);
    const m = Math.floor((convert % 3600) / 60);
    const s = Math.floor((convert % 3600) % 60);
    return [h.toString(), m.toString(), s.toString()].join(':');
};

const getVideoDurationInSeconds = (filePath) =>
    new Promise((resolve, reject) => {
        const command = spawn('/opt/ffmpeg/ffprobe', [
            '-v',
            'error',
            '-show_entries',
            'format=duration',
            '-of',
            'default=noprint_wrappers=1:nokey=1',
            filePath,
        ]);

        let output = '';
        command.stdout.on('data', (chunk) => (output += chunk.toString()));
        command.on('error', (error) => {
            reject(new Error(`ffprobe failed: ${error}`));
        });
        command.on('close', (code) => {
            if (code !== 0) {
                reject(new Error(`ffprobe exited with code ${code}`));
            } else {
                resolve(parseFloat(output));
            }
        });
    });

const ffmpegTransformVideo = async (
    inputFile,
    outputFile,
    rangeTime,
    width,
    height,
    bitrate
) =>
    new Promise((resolve, reject) => {
        let change = false;
        const command = ['-y', '-i', inputFile];

        if (width && height) {
            if (width > height) {
                command.push('-vf', `scale=${width}:-2`);
            } else if (width < height) {
                command.push('-vf', `scale=-2:${height}`);
            } else {
                command.push('-vf', `scale=${width}:${height}`);
            }
            change = true;
        }

        if (bitrate) {
            command.push('-b:v', `${bitrate}k`);
            change = true;
        }

        command.push(outputFile);

        const { start, end } = rangeTime;
        if (start && end) {
            const duration = end - start;
            command.unshift(
                '-t',
                secondsToHms(duration),
                '-ss',
                secondsToHms(start === '0' ? '1' : start)
            );
            change = true;
        }

        if (!change) {
            resolve(change);
        } else {
            console.log('command:', command.join(' '));
            const process = spawn('/opt/ffmpeg/ffmpeg', command);
            process.stderr.on('data', (data) => {
                console.log('STDERR: ', data.toString());
            });
            process.stdout.on('data', (data) => {
                console.error('STDOUT: ', data.toString());
            });
            process.on('error', (error) => {
                console.error('Error on process:', error);
            });
            process.on('close', (code) => {
                if (code === 0) {
                    resolve(change);
                } else {
                    reject(new Error('ffmpeg failed'));
                }
            });
        }
    });

const downloadFromS3 = async ({ file, bucket, region }) => {
    const s3 = new s3client.S3Client({ region });
    const data = await s3.send(
        new s3client.GetObjectCommand({
            Bucket: bucket,
            Key: file,
        })
    );
    const parsedFileName = parseFileName(file);
    const endFileName = join('/', 'tmp', parsedFileName.base);
    await fs.writeFile(endFileName, data.Body);
    return data.Metadata;
};

const uploadToS3 = async ({ file, key, bucket, region, metadata }) => {
    const s3 = new s3client.S3Client({ region });
    const body = await fs.readFile(file);
    await s3.send(
        new s3client.PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body,
            Metadata: metadata,
        })
    );
};

const deleteFromS3 = async ({ file, bucket, region }) => {
    const s3 = new s3client.S3Client({ region });
    await s3.send(
        new s3client.DeleteObjectCommand({
            Bucket: bucket,
            Key: file,
        })
    );
};

//
// assumption: maxsize for preview videos is < 10MB otherwise it will be enormous
//
const generateClipAndReplaceOriginal = async ({
    filename,
    bucket,
    region,
    rangeTime,
    maxSize,
    width,
    height,
}) => {
    const parsedFileName = parseFileName(filename);
    const filePath = join('/', 'tmp', parsedFileName.base);
    const tempPath = join(
        '/',
        'tmp',
        `${parsedFileName.name}_temp.${parsedFileName.ext}`
    );

    // if is need to "compress" the video, we will use bitrate
    let bitrate = null;
    let stats = await fs.stat(filePath);
    const fileSizeInMegabytes = stats.size / (1024 * 1024);
    if (fileSizeInMegabytes > maxSize) {
        const fileSizeInKb = maxSize * 1024;
        if (rangeTime.start && rangeTime.end) {
            bitrate = fileSizeInKb / (rangeTime.end - rangeTime.start);
        } else {
            const durationInSeconds = await getVideoDurationInSeconds(filePath);
            bitrate = fileSizeInKb / durationInSeconds;
        }
    }

    const res = await ffmpegTransformVideo(
        filePath,
        tempPath,
        rangeTime,
        width,
        height,
        bitrate
    );

    if (!res) {
        await notify({ filename, size: stats.size });
        return;
    }

    await fs.unlink(filePath);
    await fs.rename(tempPath, filePath);
    stats = await fs.stat(filePath);

    await uploadToS3({
        file: filePath,
        key: filename,
        bucket,
        region,
        metadata: {
            fromtransform: 'true',
        },
    });

    await fs.unlink(filePath);
    await notify({ filename, size: stats.size });
};

const generateThumb = async ({ filename, bucket, region }) => {
    const parsedFileName = parseFileName(filename);
    if (parsedFileName.name.includes('_thumb')) {
        console.log('File has already been processed, skipping');
        return;
    }

    const metadata = await downloadFromS3({
        file: filename,
        bucket,
        region,
    });

    if (metadata && metadata.fromtransform !== 'true') {
        await generateClipAndReplaceOriginal({
            filename,
            bucket,
            region,

            width: parseInt(metadata.width, 10), // Max width (comes from studio)
            height: parseInt(metadata.height, 10), // Max height (comes from Studio)
            maxSize: parseInt(metadata.maxsize, 10), // Max allowed size for this media (from Studio)

            // if it's a preview clip or not
            rangeTime: {
                start: metadata.rangetimestart
                    ? parseFloat(metadata.rangetimestart)
                    : null,
                end: metadata.rangetimeend
                    ? parseFloat(metadata.rangetimeend)
                    : null,
            },
        });
    }
};

module.exports.postprocess = async (event) => {
    await Promise.allSettled(
        event.Records.map((record) =>
            generateThumb({
                filename: record.s3.object.key,
                bucket: record.s3.bucket.name,
                region: record.awsRegion,
            })
        )
    );
};

const deleteThumb = async ({ filename, bucket, region }) => {
    try {
        const parsedFileName = parseFileName(filename);
        const endFileName = join(
            parsedFileName.dir,
            `${parsedFileName.name}_thumb.jpg`
        );
        await deleteFromS3({
            file: endFileName,
            bucket,
            region,
        });
    } catch (error) {
        console.log('DELETETHUMB - Failed:', {
            filename,
            bucket,
            region,
            error: error.message,
        });
    }
};

module.exports.postdelete = async (event) => {
    await Promise.all(
        event.Records.map((record) =>
            deleteThumb({
                filename: record.s3.object.key,
                bucket: record.s3.bucket.name,
                region: record.awsRegion,
            })
        )
    );
};
