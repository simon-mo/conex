FROM nvcr.io/nvidia/pytorch:23.09-py3
RUN pip install vllm huggingface_hub[cli] hf_transfer
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download HuggingFaceH4/zephyr-7b-alpha --include '*.safetensors'