# CUDA 11
FROM nvcr.io/nvidia/pytorch:22.12-py3
RUN pip install --use-deprecated=legacy-resolver vllm huggingface_hub[cli] hf_transfer
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00001-of-00008.safetensors
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00002-of-00008.safetensors
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00003-of-00008.safetensors
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00004-of-00008.safetensors
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00005-of-00008.safetensors
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00006-of-00008.safetensors
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00007-of-00008.safetensors
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include  model-00008-of-00008.safetensors
