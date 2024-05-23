# 构建输入数据流
import os
import csv
import random

# Query实际所需要的5 tables
dataset_files = ["lineitem.tbl", "orders.tbl", "supplier.tbl", "customer.tbl", "nation.tbl"]
# 过滤后的tables
FTABLES = set(["nation.tbl", "supplier.tbl", "customer.tbl"])

# 目标输出路径
output_path = "source_data.csv"

# 设定随机数种子为0
random.seed(0)

# 生成和处理数据集
def data_generator_for_query7():
    dataset_firstHalf = []
    dataset_lastHalf = {table: [] for table in dataset_files} # 用于存储每个表的后半部分数据
    final_dataset = [] # 最终数据集
    add_count = 0 # 添加的行数
    delete_count = 0 # 删除的行数
    total_size = 0 # 总行数

    # 加载数据阶段
    for file_name in dataset_files:
        with open(file_name, "r") as file:
            file_data = file.readlines()

        n = len(file_data)
        total_size += n
        print(f"{file_name} size is {n}")
        # 如果不是nation或region表 则数据拆分为两半
        if file_name not in FTABLES:
            file_data1 = file_data[:n // 2]
            file_data2 = file_data[n // 2:]

            for line in file_data1:
                dataset_firstHalf.append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
                # 将该行添加到最终数据集
                final_dataset.append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
                add_count += 1

            for line in file_data2:
                dataset_lastHalf[file_name].append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
        else:
            # 将所有行添加到最终数据集
            for line in file_data:
                final_dataset.append(
                    f'+|{file_name}|{line.strip()}'.replace(',', '.'))
                add_count += 1

    # 随机化处理阶段
    # 对前半部分数据集进行随机化，并执行随机删除与插入操作，保持总行数不变
    random.shuffle(dataset_firstHalf)
    while dataset_firstHalf:
        deleted_tuple = dataset_firstHalf.pop()
        table_name = deleted_tuple.split("|")[1]
    
        # 避免从过滤表中删除和插入数据
        while table_name in FTABLES or not dataset_lastHalf[table_name]:
            deleted_tuple = dataset_firstHalf.pop()
            table_name = deleted_tuple.split("|")[1]

        # 删除操作标记
        final_dataset.append(f'-{deleted_tuple[1:]}'.strip('"'))
        delete_count += 1

       # 从对应的后半部分数据集中插入一条数据
        inserted_tuple = dataset_lastHalf[table_name].pop()

        # 插入操作标记
        final_dataset.append(inserted_tuple.strip('"'))
        add_count += 1

    # 处理剩余的数据行
    for table_name, table_data in dataset_lastHalf.items():
        while table_data:
            final_dataset.append(table_data.pop())
            add_count += 1

    print(f'add count: {add_count}')
    print(f'delete count: {delete_count}')
    print(f'total size: {total_size}')

    # 将最终数据集写入CSV文件
    with open(output_path, 'w') as f:
        f.writelines("\n".join(final_dataset))
    
    print('Data generated successfully!')


if __name__ == '__main__':
    data_generator_for_query7()
