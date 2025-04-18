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
    Клас для запуску Go WebAssembly модуля через Node.js
    """

    def __init__(self, wasm_file_path="storage_timeline.wasm", wasm_exec_path="wasm_exec.js"):

        # Спробувати знайти файли в різних місцях
        # 1. За переданими шляхами
        if os.path.exists(wasm_file_path) and os.path.exists(wasm_exec_path):
            self.wasm_file_path = os.path.abspath(wasm_file_path)
            self.wasm_exec_path = os.path.abspath(wasm_exec_path)
        else:
            # 2. В директорії модуля
            module_spec = importlib.util.find_spec('storage_timeline_client')
            if module_spec and module_spec.origin:
                module_dir = os.path.dirname(os.path.abspath(module_spec.origin))

                # Спробувати знайти в директорії модуля
                wasm_file_path_local = os.path.join(module_dir, 'storage_timeline.wasm')
                wasm_exec_path_local = os.path.join(module_dir, 'wasm_exec.js')

                if os.path.exists(wasm_file_path_local) and os.path.exists(wasm_exec_path_local):
                    self.wasm_file_path = wasm_file_path_local
                    self.wasm_exec_path = wasm_exec_path_local
                else:
                    # 3. Пошук в кореневому каталозі проекту
                    current_dir = os.getcwd()
                    wasm_file_path_proj = os.path.join(current_dir, 'storage_timeline.wasm')
                    wasm_exec_path_proj = os.path.join(current_dir, 'wasm_exec.js')

                    if os.path.exists(wasm_file_path_proj) and os.path.exists(wasm_exec_path_proj):
                        self.wasm_file_path = wasm_file_path_proj
                        self.wasm_exec_path = wasm_exec_path_proj
                    else:
                        # Якщо все невдало, повідомити про помилку
                        raise FileNotFoundError(
                            "Could not find WASM files. Please ensure 'storage_timeline.wasm' and 'wasm_exec.js' "
                            "are placed in the current directory or package installation directory."
                        )

        # Ініціалізація інших атрибутів
        self.node_script_path = None
        self.initialize()

    def initialize(self):
        """Ініціалізує Node.js середовище для запуску Go WASM модуля"""
        # Створюємо JavaScript код для завантаження та ініціалізації Go WASM модуля
        node_script = """
        const fs = require('fs');

        // Завантажуємо wasm_exec.js, який надає Go runtime для WASM
        const wasmExecPath = process.argv[2];
        eval(fs.readFileSync(wasmExecPath, 'utf8'));

        // Шлях до WASM файлу
        const wasmPath = process.argv[3];

        // Шляхи до файлів вводу/виводу
        const inputFilePath = process.argv[4];
        const outputFilePath = process.argv[5];
        const encodeType = process.argv[6] || ''; // 'base64' або порожній рядок

        // Функція для запуску основної логіки після завантаження WASM
        async function runWasm() {
            try {
                // Створюємо Go середовище
                const go = new Go();

                // Завантажуємо та інстанціюємо WASM модуль
                const wasmInstance = await WebAssembly.instantiate(
                    fs.readFileSync(wasmPath), 
                    go.importObject
                );

                // Запускаємо Go середовище
                go.run(wasmInstance.instance);

                // Читаємо дані з файлу
                let inputData = fs.readFileSync(inputFilePath);

                // Якщо вказано base64, декодуємо дані
                if (encodeType === 'base64') {
                    inputData = Buffer.from(inputData.toString(), 'base64');
                }

                // Викликаємо функцію parse з Timeline
                const result = StorageTimeline.Timeline.parse(new Uint8Array(inputData));

                // Записуємо результат у файл виводу
                fs.writeFileSync(outputFilePath, JSON.stringify(result, null, 2));

                process.exit(0);
            } catch (error) {
                console.error('Error in WASM execution:', error);
                process.exit(1);
            }
        }

        // Запускаємо основну логіку
        runWasm().catch(err => {
            console.error('Fatal error:', err);
            process.exit(1);
        });
        """

        # Створюємо тимчасовий файл для скрипта Node.js
        fd, self.node_script_path = tempfile.mkstemp(suffix='.js')
        os.write(fd, node_script.encode('utf-8'))
        os.close(fd)

    def parse_timeline(self, binary_data):
        """
        Обробляє бінарні дані за допомогою Go WASM модуля
        """
        try:
            # Кодуємо бінарні дані в base64 для уникнення проблем із передачею
            encoded_data = base64.b64encode(binary_data)

            # Змінимо скрипт Node.js на льоту для передачі даних через файл
            temp_input_file = tempfile.NamedTemporaryFile(delete=False, suffix='.bin')
            temp_input_path = temp_input_file.name
            temp_input_file.write(encoded_data)
            temp_input_file.close()

            temp_output_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
            temp_output_path = temp_output_file.name
            temp_output_file.close()

            # Запускаємо Node.js процес із скриптом та шляхами до файлів
            process = subprocess.Popen(
                [
                    'node', self.node_script_path,
                    self.wasm_exec_path, self.wasm_file_path,
                    temp_input_path, temp_output_path, 'base64'
                ],
                stderr=subprocess.PIPE
            )

            # Чекаємо завершення процесу
            _, stderr = process.communicate()

            if process.returncode != 0:
                # Помилка виконання
                try:
                    error_message = stderr.decode('utf-8', errors='replace')
                except:
                    error_message = "Невідома помилка"

                raise Exception(f"Помилка при обробці даних WASM: {error_message}")

            # Читаємо результат з файлу
            try:
                with open(temp_output_path, 'r') as f:
                    result_data = f.read()
                    return json.loads(result_data)
            except json.JSONDecodeError as json_err:
                raise Exception(f"Помилка декодування JSON: {str(json_err)}. Перші 100 символів: {result_data[:100]}")
            finally:
                # Видаляємо тимчасові файли
                try:
                    os.unlink(temp_input_path)
                    os.unlink(temp_output_path)
                except:
                    pass

        except Exception as e:
            raise Exception(f"Помилка при виконанні WASM: {str(e)}")

    def __del__(self):
        """Прибираємо тимчасові файли при знищенні об'єкта"""
        if self.node_script_path and os.path.exists(self.node_script_path):
            try:
                os.remove(self.node_script_path)
            except:
                pass


# Клас для роботи з часовими рядами з підтримкою WASM
class TimeLine:
    def __init__(self, schema, name, binary=False):
        self.schema = schema
        self.name = name
        self.binary = binary

    def _process_response(self, response):
        """Обробляє відповідь від сервера, перевіряючи на бінарний формат"""
        data = response.read()
        content_type = response.headers.get('Content-Type', '')

        if self.binary and 'application/storage-timeline' in content_type:
            # Використовуємо WASM для аналізу бінарних даних
            return self.schema.storage.wasm_runner.parse_timeline(data)
        else:
            # Звичайна JSON відповідь
            return json.loads(data.decode('utf-8'))

    def all_numbers(self):
        """Отримати всі числові значення з часового ряду"""
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
        """Отримати всі рядкові значення з часового ряду"""
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
        """Отримати всі документи з часового ряду"""
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

            # Аналіз JSON документів у відповіді
            for item in data:
                try:
                    item["value"] = json.loads(item["value"])
                except:
                    item["value"] = None

            return data

    def add_number(self, value, time=None):
        """Додати числове значення до часового ряду"""
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
        # Не додаємо заголовок для запитів додавання даних

        response = urllib.request.urlopen(request, context=ssl_context)
        return self._process_response(response)

    def add_string(self, value, time=None):
        """Додати рядкове значення до часового ряду"""
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
        # Не додаємо заголовок для запитів додавання даних

        response = urllib.request.urlopen(request, context=ssl_context)
        return self._process_response(response)


# Клас для роботи зі схемами даних
class Schema:
    def __init__(self, storage, name, binary=False):
        self.storage = storage
        self.name = name
        self.binary = binary

    def list(self):
        """Отримати список часових рядів у схемі"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.storage.uri):
            uri_string = f"{self.storage.uri}?action=schema-list&schema={self.name}"
        else:
            uri_string = f"{self.storage.uri}/schema/list?schema={self.name}"

        request = urllib.request.Request(uri_string)
        # Не додаємо заголовок для запитів списків

        with urllib.request.urlopen(request, context=ssl_context) as url:
            data = json.loads(url.read().decode())
            return data

    def time_line(self, name):
        """Отримати об'єкт часового ряду"""
        return TimeLine(self, name, self.binary)


# Головний клас для роботи зі сховищем даних
class Storage:
    def __init__(self, uri, binary=False, wasm_file="storage_timeline.wasm", wasm_exec="wasm_exec.js"):
        """
        Ініціалізувати сховище даних

        Args:
            uri: URI сервера сховища
            binary: Чи використовувати бінарний формат (використовує WASM для обробки)
            wasm_file: Шлях до WASM файлу
            wasm_exec: Шлях до wasm_exec.js файлу
        """
        self.uri = uri.rstrip('/')
        self.binary = binary
        self.wasm_runner = GoWasmRunner(wasm_file, wasm_exec) if binary else None

    def schema(self, name):
        """Отримати об'єкт схеми даних"""
        return Schema(self, name, self.binary)

    def list(self):
        """Отримати список схем даних у сховищі"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if is_v2_api(self.uri):
            uri_string = f"{self.uri}?action=storage-list"
        else:
            uri_string = f"{self.uri}/storage/list"

        request = urllib.request.Request(uri_string)
        # Не додаємо заголовок для запитів списків

        with urllib.request.urlopen(request, context=ssl_context) as url:
            data = json.loads(url.read().decode())
            return data