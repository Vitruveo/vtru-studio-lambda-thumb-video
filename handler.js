/* eslint-disable no-return-assign */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-console */
const axios = require('axios');
const { spawn } = require('child_process');
const { parse: parseFileName, join } = require('path');
const s3client = require('@aws-sdk/client-s3');
const fs = require('fs/promises');

const notify = async ({ filename, size }) => {
    const url = `${process.env.NOTIFY_API_URL}/assets/notify/file`;
    const data = { filename, size };

    console.log('url:', url);

    try {
        const response = await axios.put(url, data);
        console.log(`Status: ${response.status}`);
        console.log('Body: ', response.data);
    } catch (error) {
        console.error(`Error: ${error}`);
    }
};

const secondsToHms = (d) => {
    const convert = Number(d);
    const h = Math.floor(convert / 3600);
    const m = Math.floor((convert % 3600) / 60);
    const s = Math.floor((convert % 3600) % 60);

    const hDisplay = h > 0 ? h + (h === 1 ? ':' : ':') : '';
    const mDisplay = m > 0 ? m + (m === 1 ? ':' : ':') : '';
    const sDisplay = s > 0 ? s : '';
    return hDisplay + mDisplay + sDisplay;
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

        command.stdout.on('data', (chunk) => (output += chunk));
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
        const command = ['-y', '-i', inputFile];
        let change = false;

        if (width && height) {
            command.push('-vf', `scale=${width}:${height}`);
            change = true;
        }

        if (bitrate) {
            command.push('-b:v', `${bitrate}k`);
            change = true;
        }

        command.push(outputFile);

        if (rangeTime?.start && rangeTime?.end) {
            const { start, end } = rangeTime;
            console.log('start:', start);
            console.log('end:', end);
            const duration = parseInt(end, 10) - parseInt(start, 10);
            command.unshift(secondsToHms(duration));
            command.unshift('-t');
            command.unshift(secondsToHms(start === '0' ? '1' : start));
            command.unshift('-ss');
            change = true;
        }

        if (!change) {
            resolve(change);
        } else {
            console.log('command:', command.join(' '));

            const process = spawn('/opt/ffmpeg/ffmpeg', command);

            process.stderr.on('data', (data) => {
                console.log(data.toString());
            });

            process.stdout.on('data', (data) => {
                console.error(data.toString());
            });

            process.on('error', (error) => {
                console.error('Error from process:', error);
            });

            process.stderr.on('error', (error) => {
                console.error('Error from stderr:', error);
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
    const endFileName = join('/', 'tmp', file);
    const parsedFileName = parseFileName(endFileName);
    await fs.mkdir(parsedFileName.dir, { recursive: true });
    await fs.writeFile(join('/', 'tmp', file), data.Body);

    return data.Metadata;
};

const uploadToS3 = async ({ file, key, bucket, region }) => {
    const s3 = new s3client.S3Client({ region });
    const body = await fs.readFile(join('/', 'tmp', file));
    await s3.send(
        new s3client.PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body,
        })
    );
};

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

    console.log('filename:', filename);
    console.log('rangeTime:', rangeTime);

    const tempFilename = `${parsedFileName.name}_temp.mp4`;

    console.log('tempFilename:', tempFilename);

    console.log('width:', width, 'height:', height);

    const filePath = join('/', 'tmp', filename);

    let stats = await fs.stat(join('/', 'tmp', filename));
    let bitrate = null;
    const fileSizeInBytes = stats.size;
    const fileSizeInMegabytes = fileSizeInBytes / (1024 * 1024);

    console.log(
        `meu video: ${fileSizeInMegabytes} é maior que ${maxSize}?`,
        fileSizeInMegabytes > maxSize
    );

    if (fileSizeInMegabytes > maxSize) {
        const fileSizeInKb = Number.parseInt(maxSize, 10) * 8192;
        if (rangeTime?.start && rangeTime?.end) {
            bitrate =
                fileSizeInKb /
                (parseInt(rangeTime.end, 10) - parseInt(rangeTime.start, 10));
        } else {
            const durationInSeconds = await getVideoDurationInSeconds(filePath);
            bitrate = fileSizeInKb / durationInSeconds;
        }
    }

    console.log('bitrate:', bitrate);

    const res = await ffmpegTransformVideo(
        filePath,
        join('/', 'tmp', tempFilename),
        rangeTime,
        width,
        height,
        bitrate
    );

    console.log('passei aqui:', res);

    if (!res) {
        await notify({ filename, size: stats.size });
        return;
    }

    await fs.rename(join('/', 'tmp', tempFilename), join('/', 'tmp', filename));

    stats = await fs.stat(join('/', 'tmp', filename));

    console.log(
        'filename:',
        filename,
        'key:',
        join(parsedFileName.dir, filename),
        'size:',
        stats.size
    );

    await uploadToS3({
        file: filename,
        key: filename,
        bucket,
        region,
    });

    await notify({ filename, size: stats.size });
};

const generateThumb = async ({ filename, bucket, region }) => {
    console.log('filename:', filename);
    const parsedFileName = parseFileName(filename);
    console.log('parsedFileName:', parsedFileName);

    if (parsedFileName.name.endsWith('_thumb')) {
        console.log('File has already been processed, skipping');
        return;
    }

    const metadata = await downloadFromS3({
        file: filename,
        bucket,
        region,
    });

    console.log('metadata:', metadata);

    if (metadata && Object.values(metadata)?.length > 0) {
        await generateClipAndReplaceOriginal({
            filename,
            bucket,
            region,
            width: metadata.width,
            height: metadata.height,
            maxSize: metadata.maxsize,
            rangeTime: {
                start: metadata.rangetimestart,
                end: metadata.rangetimeend,
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
        console.log(
            `[DeleteThumb] Starting thumbnail deletion. Filename: ${filename}, Bucket: ${bucket}, Region: ${region}`
        );

        const parsedFileName = parseFileName(filename);
        const thumbFilename = `${parsedFileName.name}_thumb.jpg`;
        const endFileName = join(parsedFileName.dir, thumbFilename);
        const s3 = new s3client.S3Client({ region });
        await s3.send(
            new s3client.DeleteObjectCommand({
                Bucket: bucket,
                Key: endFileName,
            })
        );

        console.log(
            `[DeleteThumb] Thumbnail deletion completed. Filename: ${endFileName}, Bucket: ${bucket}, Region: ${region}`
        );
    } catch (error) {
        console.log(error);
    }
};

// deleta o thumbnail a partir do nome do arquivo original
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
