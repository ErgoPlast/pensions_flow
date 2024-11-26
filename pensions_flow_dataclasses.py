from typing import Tuple
from datetime import datetime, timedelta
import pandas as pd

class MergeExcelData:
    def __init__(self, file_path: str, reporting_date: str, max_age: float) -> None:
        self.file_path = file_path
        self.reporting_date = pd.to_datetime(reporting_date, format='%d.%m.%Y')
        self.max_age = max_age

    @staticmethod
    def merge_contracts_pensions(file_path: str) -> pd.DataFrame:
        """
        Объединение договоров и пенсий
        """
        data_contracts = pd.read_excel(file_path, sheet_name='Договоры участников')
        data_pensions = pd.read_excel(file_path, sheet_name='Суммы пенсий')
        merged_data = pd.merge(data_contracts, data_pensions, on='Номер договора')
        return merged_data
    @staticmethod
    def calculate_retirement_date(dob: pd.Timestamp, pension_age: pd.Timestamp) -> pd.Timestamp:
        """
        Функция для расчета даты выхода на пенсию
        """
        return dob + pd.DateOffset(years=pension_age)
    @staticmethod
    def calculate_end_date(dob: pd.Timestamp, max_age: int) -> pd.Timestamp:
        """
        Функция для расчета даты окончания выплат
        """
        return dob + pd.DateOffset(years=max_age)
    def determine_contract_stage(self, row: pd.Series,
                                 current_date: pd.Timestamp) -> Tuple[str, pd.Timestamp]:
        """
        Функция для определения этапа договора
        """
        retirement_date = row['Дата выхода на пенсию']
        if current_date < retirement_date:
            return 'накопление', retirement_date
        else:
            return 'выплата', current_date
    def merge_data(self) -> pd.DataFrame:
        """
        Объединение фреймов для последующего расчета
        """
        merged_data = self.merge_contracts_pensions(self.file_path)
        merged_data['Дата рождения участника'] = pd.to_datetime(merged_data['Дата рождения участника'])
        merged_data['Дата выхода на пенсию'] = merged_data.apply(
            lambda row: self.calculate_retirement_date(row['Дата рождения участника'], 
                                                       row['Пенсионный возраст']), axis=1)
        merged_data['Дата окончания выплат'] = merged_data['Дата рождения участника'].apply(
            lambda dob: self.calculate_end_date(dob, self.max_age))
        merged_data[['Contract Stage', 'Adjusted Retirement Date']] = merged_data.apply(
            lambda row: pd.Series(self.determine_contract_stage(row, self.reporting_date)), axis=1)
        return merged_data


class ProcessPensionsFlow:
    def __init__(self, merged_data: pd.DataFrame, p_survive: float, p_t_contract: float, rate: float, output_path: str) -> None:
        self.merged_data = merged_data
        self.p_survive = p_survive
        self.p_t_contract = p_t_contract
        self.rate = rate
        self.output_path = output_path

    def get_next_payment_date(self, payment_date: pd.Timestamp) -> pd.Timestamp:
        """
        Функция расчета последующего дня выплат с учетом правила "того же дня"
        """
        # Добавляем 1 месяц
        year = payment_date.year
        month = payment_date.month + 1

        # Если выходим за декабрь, корректируем год
        if month > 12:
            month = 1
            year += 1

        # Проверяем, был ли исходный день концом месяца
        last_day_of_current_month = (payment_date.replace(day=1) +
                                    timedelta(days=32)).replace(day=1) - timedelta(days=1)
        is_end_of_month = payment_date.day == last_day_of_current_month.day

        # Если это конец месяца, дата следующего платежа — конец следующего месяца
        if is_end_of_month:
            next_month = datetime(year, month, 1)
            return (next_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        # Если это не конец месяца, возвращаем ту же дату в следующем месяце
        try:
            return payment_date.replace(year=year, month=month)
        except ValueError:
            # В случае, если следующего месяца не хватает
            return (datetime(year, month, 1) + timedelta(days=32)).replace(day=payment_date.day)        
    def process(self) -> pd.DataFrame:
        """
        Процесс создания потока пенсий
        """
        # Результаты потока пенсий
        results = []

        #   Рассчитываем ежемесячные выплаты пенсий для каждого договора
        for _, row in self.merged_data.iterrows():
            contract_id = row['Номер договора']
            pension_start_date = row['Adjusted Retirement Date']
            pension_end_date = row['Дата окончания выплат']
            initial_pension = row['Установленный размер пенсии']

            # Начальная сумма пенсии
            current_pension = initial_pension
            payment_date = pension_start_date

            while payment_date <= pension_end_date:

                # Взвешивание пенсии с учетом вероятности
                adjusted_pension = current_pension * self.p_survive * (1 - self.p_t_contract)

                results.append({
                    'Номер договора': contract_id,
                    'Дата платежа': payment_date,
                    'Размер пенсии': round(adjusted_pension, 2)
                })

                # Переход к следующей дате выплаты
                payment_date = self.get_next_payment_date(payment_date)

                # Применение индексации каждый январь
                if payment_date.month == 1:
                    current_pension *= (1 + self.rate)

        # Преобразуем результаты в DataFrame
        pension_flows = pd.DataFrame(results)

        # Сохранение результатов в новый Excel файл
        output_path = self.output_path
        pension_flows.to_excel(output_path, index=False)
        pension_flows.head()
        return pension_flows
# Замените file_path до полного пути директории с файлом
create_data = MergeExcelData(file_path = r".\datas.xlsx",
                             reporting_date='31.01.2024', max_age=100)
merge_data = create_data.merge_data()
create_pensions_flow = ProcessPensionsFlow(merged_data=merge_data, p_survive=1.0,
                                           p_t_contract=0.0, rate=0.1,
                                           output_path=r".\pensions_flow999.xlsx")
create_pensions_flow.process()