from typing import Tuple
from datetime import datetime, timedelta
import pandas as pd

# Загрузка данных из Excel
file_path = r"./Данные.xlsx"  # Замените на полный путь к вашему файлу
data_contracts = pd.read_excel(file_path, sheet_name='Договоры участников')
data_pensions = pd.read_excel(file_path, sheet_name='Суммы пенсий')

REPORTING_DATE = pd.to_datetime('31.01.2024', format='%d.%m.%Y')
RATE = 0.1
MAX_AGE = 100
P_SURVIVE = 1.0
P_T_CONTRACT = 0.0

# Объединение таблиц по номеру договора
merged_data = pd.merge(data_contracts, data_pensions, on='Номер договора')

def calculate_retirement_date(dob: pd.Timestamp, pension_age: pd.Timestamp) -> pd.Timestamp:
    """
    Функция для расчета даты выхода на пенсию
    """
    return dob + pd.DateOffset(years=pension_age)

def calculate_end_date(dob: pd.Timestamp, max_age: int) -> pd.Timestamp:
    """
    Функция для расчета даты окончания выплат
    """
    return dob + pd.DateOffset(years=max_age)

# Добавление колонок для даты выхода на пенсию и даты окончания выплат
merged_data['Дата рождения участника'] = pd.to_datetime(merged_data['Дата рождения участника'])
merged_data['Дата выхода на пенсию'] = merged_data.apply(
    lambda row: calculate_retirement_date(row['Дата рождения участника'], 
                                          row['Пенсионный возраст']), axis=1
)

merged_data['Дата окончания выплат'] = merged_data['Дата рождения участника'].apply(
    lambda dob: calculate_end_date(dob, MAX_AGE)
)

def determine_contract_stage(row: pd.Series, current_date: pd.Timestamp) -> Tuple[str, pd.Timestamp]:
    """
    Функция для определения этапа договора
    """
    retirement_date = row['Дата выхода на пенсию']
    if current_date < retirement_date:
        return 'накопление', retirement_date
    else:
        return 'выплата', current_date


def get_next_payment_date(payment_date: pd.Timestamp) -> pd.Timestamp:
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

# Добавление этапа договора
merged_data[['Contract Stage', 'Adjusted Retirement Date']] = merged_data.apply(
    lambda row: pd.Series(determine_contract_stage(row, REPORTING_DATE)), axis=1
)

# Результаты потока пенсий
results = []

# Рассчитываем ежемесячные выплаты пенсий для каждого договора
for _, row in merged_data.iterrows():
    contract_id = row['Номер договора']
    pension_start_date = row['Adjusted Retirement Date']
    pension_end_date = row['Дата окончания выплат']
    initial_pension = row['Установленный размер пенсии']

    # Начальная сумма пенсии
    current_pension = initial_pension
    payment_date = pension_start_date

    while payment_date <= pension_end_date:

        # Взвешивание пенсии с учетом вероятности
        adjusted_pension = current_pension * P_SURVIVE * (1 - P_T_CONTRACT)

        results.append({
            'Номер договора': contract_id,
            'Дата платежа': payment_date,
            'Размер пенсии': round(adjusted_pension, 2)
        })

        # Переход к следующей дате выплаты
        payment_date = get_next_payment_date(payment_date)

        # Применение индексации каждый январь
        if payment_date.month == 1:
            current_pension *= (1 + RATE)

# Преобразуем результаты в DataFrame
pension_flows = pd.DataFrame(results)

# Сохранение результатов в новый Excel файл
output_path = r"./result_flow.xlsx" # Замените на полный путь до вашей директории
pension_flows.to_excel(output_path, index=False)
pension_flows.head()
