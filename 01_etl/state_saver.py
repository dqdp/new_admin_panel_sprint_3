import abc
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, key: str, value: Any) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, key: str, value: Any) -> None:
        """Сохранить состояние в постоянное хранилище"""
        with open(self.file_path, "r+") as f:
            try:
                state = json.load(f) or {}
                state[key] = value
                f.seek(0)
                f.truncate()
                json.dump(state, f)
            except json.JSONDecodeError:
                f.seek(0)
                f.truncate()
                json.dump({key: value}, f)

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        with open(self.file_path, "w+") as f:
            try:
                state = json.load(f)
                return state if state else {}
            except json.JSONDecodeError:
                return {}


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы
    постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние по ключу и значению"""
        self.storage.save_state(key, value)

    def get_state(self) -> dict:
        """Получить полное состояние"""
        return self.storage.retrieve_state()
