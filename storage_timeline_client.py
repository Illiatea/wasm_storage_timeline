import ssl
import json
import urllib.parse
import urllib.request
import os
import subprocess
import tempfile
import base64
import importlib.util


def is_v2_api(value):
    return "cloudfunctions.net" in value


class GoWasmRunner:
    """
    Class for running Go WebAssembly module through Node.js
    """

    def __init__(self, wasm_file_path="storage_timeline.wasm", wasm_exec_path="wasm_exec.js"):

        # Try to find files in different locations
        # 1. Using provided paths
        if os.path.exists(wasm_file_path) and os.path.exists(wasm_exec_path):
            self.wasm_file_path = os.path.abspath(wasm_file_path)
            self.wasm_exec_path = os.path.abspath(wasm_exec_path)
        else:
            # 2. In the module directory
            module_spec = importlib.util.find_spec('storage_timeline_client')
            if module_spec and module_spec.origin:
                module_dir = os.path.dirname(os.path.abspath(module_spec.origin))

                # Try to find in module directory
                wasm_file_path_local = os.path.join(module_dir, 'storage_timeline.wasm')
                wasm_exec_path_local = os.path.join(module_dir, 'wasm_exec.js')

                if os.path.exists(wasm_file_path_local) and os.path.exists(wasm_exec_path_local):
                    self.wasm_file_path = wasm_file_path_local
                    self.wasm_exec_path = wasm_exec_path_local
                else:
                    # 3. Search in project root directory
                    current_dir = os.getcwd()
                    wasm_file_path_proj = os.path.join(current_dir, 'storage_timeline.wasm')
                    wasm_exec_path_proj = os.path.join(current_dir, 'wasm_exec.js')

                    if os.path.exists(wasm_file_path_proj) and os.path.exists(wasm_exec_path_proj):
                        self.wasm_file_path = wasm_file_path_proj
                        self.wasm_exec_path = wasm_exec_path_proj
                    else:
                        # If all fails, report an error
                        raise FileNotFoundError(
                            "Could not find WASM files. Please ensure 'storage_timeline.wasm' and 'wasm_exec.js' "
                            "are placed in the current directory or package installation directory."
                        )

        # Initialize other attributes
        self.node_script_path = None
        self.initialize()

    def initialize(self):
        """Initializes Node.js environment for running Go WASM module"""
        # Create JavaScript code for loading and initializing Go WASM module
        node_script = """
        const fs = require('fs');

        // Load wasm_exec.js, which provides Go runtime for WASM
        const wasmExecPath = process.argv[2];
        eval(fs.readFileSync(wasmExecPath, 'utf8'));

        // Path to WASM file
        const wasmPath = process.argv[3];

        // Paths to input/output files
        const inputFilePath = process.argv[4];
        const outputFilePath = process.argv[5];
        const encodeType = process.argv[6] || ''; // 'base64' or empty string

        // Function to run main logic after loading WASM
        async function runWasm() {
            try {
                // Create Go environment
                const go = new Go();

                // Load and instantiate WASM module
                const wasmInstance = await WebAssembly.instantiate(
                    fs.readFileSync(wasmPath), 
                    go.importObject
                );

                // Run Go environment
                go.run(wasmInstance.instance);

                // Read data from file
                let inputData = fs.readFileSync(inputFilePath);

                // If base64 is specified, decode the data
                if (encodeType === 'base64') {
                    inputData = Buffer.from(inputData.toString(), 'base64');
                }

                // Call the parse function from Timeline
                const result = StorageTimeline.Timeline.parse(new Uint8Array(inputData));

                // Write result to output file
                fs.writeFileSync(outputFilePath, JSON.stringify(result, null, 2));

                process.exit(0);
            } catch (error) {
                console.error('Error in WASM execution:', error);
                process.exit(1);
            }
        }

        // Run main logic
        runWasm().catch(err => {
            console.error('Fatal error:', err);
            process.exit(1);
        });
        """

        # Create temporary file for Node.js script
        fd, self.node_script_path = tempfile.mkstemp(suffix='.js')
        os.write(fd, node_script.encode('utf-8'))
        os.close(fd)

    def parse_timeline(self, binary_data):
        """
        Process binary data using Go WASM module
        """
        try:
            # Encode binary data in base64 to avoid transfer issues
            encoded_data = base64.b64encode(binary_data)

            # Change Node.js script on the fly to pass data through file
            temp_input_file = tempfile.NamedTemporaryFile(delete=False, suffix='.bin')
            temp_input_path = temp_input_file.name
            temp_input_file.write(encoded_data)
            temp_input_file.close()

            temp_output_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
            temp_output_path = temp_output_file.name
            temp_output_file.close()

            # Run Node.js process with script and file paths
            process = subprocess.Popen(
                [
                    'node', self.node_script_path,
                    self.wasm_exec_path, self.wasm_file_path,
                    temp_input_path, temp_output_path, 'base64'
                ],
                stderr=subprocess.PIPE
            )

            # Wait for process completion
            _, stderr = process.communicate()

            if process.returncode != 0:
                # Execution error
                try:
                    error_message = stderr.decode('utf-8', errors='replace')
                except:
                    error_message = "Unknown error"

                raise Exception(f"Error processing WASM data: {error_message}")

            # Read result from file
            try:
                with open(temp_output_path, 'r') as f:
                    result_data = f.read()
                    return json.loads(result_data)
            except json.JSONDecodeError as json_err:
                raise Exception(f"JSON decoding error: {str(json_err)}. First 100 characters: {result_data[:100]}")
            finally:
                # Delete temporary files
                try:
                    os.unlink(temp_input_path)
                    os.unlink(temp_output_path)
                except:
                    pass

        except Exception as e:
            raise Exception(f"Error executing WASM: {str(e)}")

    def __del__(self):
        """Clean up temporary files when object is destroyed"""
        if self.node_script_path and os.path.exists(self.node_script_path):
            try:
                os.remove(self.node_script_path)
            except:
                pass


# Class for working with time series with WASM support
class TimeLine:
    def __init__(self, schema, name, binary=False):
        self.schema = schema
        self.name = name
        self.binary = binary

    def _process_response(self, response):
        """Process server response, checking for binary format"""
        data = response.read()
        content_type = response.headers.get('Content-Type', '')

        if self.binary and 'application/storage-timeline' in content_type:
            # Use WASM to analyze binary data
            return self.schema.storage.wasm_runner.parse_timeline(data)
        else:
            # Regular JSON response
            return json.loads(data.decode('utf-8'))

    def all_numbers(self):
        """Get all numeric values from the timeline"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.schema.storage.uri):
            uri_string = f"{self.schema.storage.uri}?format=number&schema={self.schema.name}&timeLine={self.name}"
        else:
            uri_string = f"{self.schema.storage.uri}/timeline/all/numbers?schema={self.schema.name}&timeLine={self.name}"

        request = urllib.request.Request(uri_string)
        if self.binary:
            request.add_header('Content-Type', 'application/storage-timeline')

        with urllib.request.urlopen(request, context=ssl_context) as response:
            return self._process_response(response)

    def all_strings(self):
        """Get all string values from the timeline"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.schema.storage.uri):
            uri_string = f"{self.schema.storage.uri}?format=string&schema={self.schema.name}&timeLine={self.name}"
        else:
            uri_string = f"{self.schema.storage.uri}/timeline/all/strings?schema={self.schema.name}&timeLine={self.name}"

        request = urllib.request.Request(uri_string)
        if self.binary:
            request.add_header('Content-Type', 'application/storage-timeline')

        with urllib.request.urlopen(request, context=ssl_context) as response:
            return self._process_response(response)

    def all_documents(self):
        """Get all documents from the timeline"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.schema.storage.uri):
            uri_string = f"{self.schema.storage.uri}?format=string&schema={self.schema.name}&timeLine={self.name}"
        else:
            uri_string = f"{self.schema.storage.uri}/timeline/all/strings?schema={self.schema.name}&timeLine={self.name}"

        request = urllib.request.Request(uri_string)
        if self.binary:
            request.add_header('Content-Type', 'application/storage-timeline')

        with urllib.request.urlopen(request, context=ssl_context) as response:
            data = self._process_response(response)

            # Parse JSON documents in response
            for item in data:
                try:
                    item["value"] = json.loads(item["value"])
                except:
                    item["value"] = None

            return data

    def add_number(self, value, time=None):
        """Add numeric value to timeline"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.schema.storage.uri):
            uri_string = f"{self.schema.storage.uri}"
            data = {
                "format": "number",
                "schema": self.schema.name,
                "timeLine": self.name,
                "value": value
            }
        else:
            uri_string = f"{self.schema.storage.uri}/timeline/add/number"
            data = {
                "schema": self.schema.name,
                "timeLine": self.name,
                "value": value
            }

        if time is not None:
            data["time"] = time

        encoded_data = urllib.parse.urlencode(data).encode()

        request = urllib.request.Request(uri_string, data=encoded_data)
        # Don't add header for data addition requests

        response = urllib.request.urlopen(request, context=ssl_context)
        return self._process_response(response)

    def add_string(self, value, time=None):
        """Add string value to timeline"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.schema.storage.uri):
            uri_string = f"{self.schema.storage.uri}"
            data = {
                "format": "string",
                "schema": self.schema.name,
                "timeLine": self.name,
                "value": value
            }
        else:
            uri_string = f"{self.schema.storage.uri}/timeline/add/string"
            data = {
                "schema": self.schema.name,
                "timeLine": self.name,
                "value": value
            }

        if time is not None:
            data["time"] = time

        encoded_data = urllib.parse.urlencode(data).encode()

        request = urllib.request.Request(uri_string, data=encoded_data)
        # Don't add header for data addition requests

        response = urllib.request.urlopen(request, context=ssl_context)
        return self._process_response(response)


# Class for working with data schemas
class Schema:
    def __init__(self, storage, name, binary=False):
        self.storage = storage
        self.name = name
        self.binary = binary

    def list(self):
        """Get list of timelines in the schema"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.storage.uri):
            uri_string = f"{self.storage.uri}?action=schema-list&schema={self.name}"
        else:
            uri_string = f"{self.storage.uri}/schema/list?schema={self.name}"

        request = urllib.request.Request(uri_string)
        # Don't add header for list requests

        with urllib.request.urlopen(request, context=ssl_context) as url:
            data = json.loads(url.read().decode())
            return data

    def time_line(self, name):
        """Get timeline object"""
        return TimeLine(self, name, self.binary)


# Main class for working with data storage
class Storage:
    def __init__(self, uri, binary=False, wasm_file="storage_timeline.wasm", wasm_exec="wasm_exec.js"):
        """
        Initialize data storage

        Args:
            uri: Storage server URI
            binary: Whether to use binary format (uses WASM for processing)
            wasm_file: Path to WASM file
            wasm_exec: Path to wasm_exec.js file
        """
        self.uri = uri.rstrip('/')
        self.binary = binary
        self.wasm_runner = GoWasmRunner(wasm_file, wasm_exec) if binary else None

    def schema(self, name):
        """Get data schema object"""
        return Schema(self, name, self.binary)

    def list(self):
        """Get list of data schemas in storage"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.uri):
            uri_string = f"{self.uri}?action=storage-list"
        else:
            uri_string = f"{self.uri}/storage/list"

        request = urllib.request.Request(uri_string)
        # Don't add header for list requests

        with urllib.request.urlopen(request, context=ssl_context) as url:
            data = json.loads(url.read().decode())
            return data