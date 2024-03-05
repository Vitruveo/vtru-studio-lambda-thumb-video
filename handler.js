/* eslint-disable no-console */
const { spawn } = require('child_process');
const { parse: parseFileName, join } = require('path');
const s3client = require('@aws-sdk/client-s3');
const fs = require('fs/promises');

const ffmpeg = async (inputFile, outputFile) =>
    new Promise((resolve, reject) => {
        const process = spawn('/opt/ffmpeg/ffmpeg', [
            '-i',
            inputFile,
            '-vf',
            'thumbnail',
            '-qscale:v',
            '3',
            '-frames:v',
            '1',
            outputFile,
        ]);

        process.stderr.on('data', (data) => {
            console.log(data.toString());
        });

        process.stdout.on('data', (data) => {
            console.error(data.toString());
        });

        process.on('close', (code) => {
            if (code === 0) {
                resolve();
            } else {
                reject(new Error('ffmpeg failed'));
            }
        });
    });

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

const ffmpegClip = async (inputFile, outputFile, rangeTime) =>
    new Promise((resolve, reject) => {
        const command = ['-i', inputFile, '-c', 'copy', outputFile];

        if (rangeTime) {
            const { start, end } = rangeTime;
            console.log('start:', start);
            console.log('end:', end);
            const duration = parseInt(end, 10) - parseInt(start, 10);
            command.unshift(secondsToHms(duration));
            command.unshift('-t');
            command.unshift(secondsToHms(start));
            command.unshift('-ss');
        }

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
                resolve();
            } else {
                reject(new Error('ffmpeg failed'));
            }
        });
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

const generateClipAndUpload = async ({
    filename,
    bucket,
    region,
    rangeTime,
}) => {
    const parsedFileName = parseFileName(filename);
    const thumbFilename = `${parsedFileName.name}_thumb.mp4`;

    const endFileName = join(parsedFileName.dir, thumbFilename);

    console.log('endfilename:', endFileName);
    console.log('rangeTime:', rangeTime);

    await ffmpegClip(
        join('/', 'tmp', filename),
        join('/', 'tmp', thumbFilename),
        rangeTime
    );

    console.log('clip generated:', join('/', 'tmp', thumbFilename));

    console.log('thumbFilename:', thumbFilename);
    console.log('endFileName:', endFileName);

    await uploadToS3({
        file: thumbFilename,
        key: endFileName,
        bucket,
        region,
    });
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

    if (metadata && metadata.rangetimestart && metadata.rangetimeend) {
        await generateClipAndUpload({
            filename,
            bucket,
            region,
            rangeTime: {
                start: metadata.rangetimestart,
                end: metadata.rangetimeend,
            },
        });
    }

    // await ffmpeg(join('/', 'tmp', filename), join('/', 'tmp', thumbFilename));
    // const endFileName = join(parsedFileName.dir, thumbFilename);
    // await uploadToS3({
    //     file: thumbFilename,
    //     key: endFileName,
    //     bucket,
    //     region,
    // });
};

module.exports.postprocess = async (event) => {
    await Promise.all(
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
