from setuptools import setup, find_packages

setup(
    name="wasm-storage-timeline",
    version="0.1.1",
    description="Client library for interacting with storage timeline services",
    author="Illiatea",
    author_email="illiatea2@gmail.com",
    py_modules=["storage_timeline_client"],
    package_data={
        "": ["storage_timeline.wasm", "wasm_exec.js"],
    },
    include_package_data=True,
    install_requires=['python-dotenv']
)