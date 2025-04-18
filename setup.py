from setuptools import setup

setup(
    name="wasm-storage-timeline",
    version="0.1.0",
    description="Client library for interacting with storage timeline services",
    author="Illiatea",
    author_email="illiatea2@gmail.com",
    py_modules=["storage_timeline_client"],  # Single Python module
    data_files=[
        ('', ['storage_timeline.wasm', 'wasm_exec.js'])  # Include these files in the package root
    ],
    install_requires=['python-dotenv']
)