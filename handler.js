const { spawn } = require('child_process');
const { parse: parseFileName, join } = require('path');
const s3client = require('@aws-sdk/client-s3');
const fs = require('fs/promises');

const ffmpeg = async (inputFile, outputFile) =>
    new Promise((resolve, reject) => {
        const process = spawn('ffmpeg', [
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
    await fs.writeFile(join('/', 'tmp', file), data.Body);
};

const uploadToS3 = async ({ file, bucket, region }) => {
    const s3 = new s3client.S3Client({ region });
    const body = await fs.readFile(join('/', 'tmp', file));
    await s3.send(
        new s3client.PutObjectCommand({
            Bucket: bucket,
            Key: file,
            Body: body,
        })
    );
};

const generateThumb = async ({ filename, bucket, region }) => {
    const parsedFileName = parseFileName(filename);
    const thumbFilename = `${parsedFileName.name}.jpg`;
    await downloadFromS3({
        file: filename,
        bucket,
        region,
    });
    await ffmpeg(join('/', 'tmp', filename), join('/', 'tmp', thumbFilename));
    await uploadToS3({
        file: thumbFilename,
        bucket,
        region,
    });
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
    const parsedFileName = parseFileName(filename);
    const thumbFilename = `${parsedFileName.name}.jpg`;
    const s3 = new s3client.S3Client({ region });
    await s3.send(
        new s3client.DeleteObjectCommand({
            Bucket: bucket,
            Key: thumbFilename,
        })
    );
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
