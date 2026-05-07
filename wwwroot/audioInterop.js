window.realtimeAudio = (() => {
    const targetRate = 24000;
    let captureContext;
    let playbackContext;
    let mediaStream;
    let sourceNode;
    let processorNode;
    let dotNetRef;
    let playbackTime = 0;
    let chunkSendChain = Promise.resolve();

    async function initPlayback() {
        playbackContext ??= new (window.AudioContext || window.webkitAudioContext)();

        if (playbackContext.state === "suspended") {
            await playbackContext.resume();
        }
    }

    async function startCapture(ref) {
        dotNetRef = ref;
        chunkSendChain = Promise.resolve();
        await initPlayback();

        try {
            mediaStream = await navigator.mediaDevices.getUserMedia({
                audio: {
                    channelCount: 1,
                    echoCancellation: true,
                    noiseSuppression: true,
                    autoGainControl: true
                }
            });

            captureContext = new (window.AudioContext || window.webkitAudioContext)();
            sourceNode = captureContext.createMediaStreamSource(mediaStream);
            processorNode = captureContext.createScriptProcessor(4096, 1, 1);

            processorNode.onaudioprocess = async (event) => {
                const input = event.inputBuffer.getChannelData(0);
                const resampled = resample(input, captureContext.sampleRate, targetRate);
                const base64 = encodePcm16(resampled);

                if (base64 && dotNetRef) {
                    chunkSendChain = chunkSendChain
                        .then(() => dotNetRef?.invokeMethodAsync("OnAudioChunk", base64))
                        .catch((error) => console.warn("Unable to send audio chunk", error))
                }
            };

            sourceNode.connect(processorNode);
            processorNode.connect(captureContext.destination);
        } catch (error) {
            await stopCapture();

            if (dotNetRef) {
                await dotNetRef.invokeMethodAsync("OnAudioError", error.message || String(error));
            }
        }
    }

    async function stopCapture() {
        if (processorNode) {
            processorNode.disconnect();
            processorNode.onaudioprocess = null;
            processorNode = null;
        }

        if (sourceNode) {
            sourceNode.disconnect();
            sourceNode = null;
        }

        if (mediaStream) {
            for (const track of mediaStream.getTracks()) {
                track.stop();
            }
            mediaStream = null;
        }

        if (captureContext) {
            await captureContext.close();
            captureContext = null;
        }

        await chunkSendChain;
    }

    async function playPcm16Base64(base64) {
        if (!base64) {
            return;
        }

        await initPlayback();

        const bytes = base64ToBytes(base64);
        const sampleCount = Math.floor(bytes.length / 2);
        const audioBuffer = playbackContext.createBuffer(1, sampleCount, targetRate);
        const channel = audioBuffer.getChannelData(0);
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

        for (let i = 0; i < sampleCount; i++) {
            channel[i] = view.getInt16(i * 2, true) / 32768;
        }

        const source = playbackContext.createBufferSource();
        source.buffer = audioBuffer;
        source.connect(playbackContext.destination);

        const startAt = Math.max(playbackContext.currentTime + 0.015, playbackTime);
        source.start(startAt);
        playbackTime = startAt + audioBuffer.duration;
    }

    function resetPlayback() {
        if (playbackContext) {
            playbackTime = playbackContext.currentTime + 0.02;
        } else {
            playbackTime = 0;
        }
    }

    function resample(samples, fromRate, toRate) {
        if (fromRate === toRate) {
            return samples;
        }

        const ratio = fromRate / toRate;
        const length = Math.round(samples.length / ratio);
        const result = new Float32Array(length);

        for (let i = 0; i < length; i++) {
            const position = i * ratio;
            const index = Math.floor(position);
            const nextIndex = Math.min(index + 1, samples.length - 1);
            const weight = position - index;
            result[i] = samples[index] * (1 - weight) + samples[nextIndex] * weight;
        }

        return result;
    }

    function encodePcm16(samples) {
        const buffer = new ArrayBuffer(samples.length * 2);
        const view = new DataView(buffer);

        for (let i = 0; i < samples.length; i++) {
            const sample = Math.max(-1, Math.min(1, samples[i]));
            view.setInt16(i * 2, sample < 0 ? sample * 0x8000 : sample * 0x7fff, true);
        }

        return bytesToBase64(new Uint8Array(buffer));
    }

    function bytesToBase64(bytes) {
        let binary = "";
        const chunkSize = 0x8000;

        for (let i = 0; i < bytes.length; i += chunkSize) {
            binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
        }

        return btoa(binary);
    }

    function base64ToBytes(base64) {
        const binary = atob(base64);
        const bytes = new Uint8Array(binary.length);

        for (let i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }

        return bytes;
    }

    return {
        initPlayback,
        startCapture,
        stopCapture,
        playPcm16Base64,
        resetPlayback
    };
})();
