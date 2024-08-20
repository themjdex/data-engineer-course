# Запрашиваем имя и фамилию
first_name = input("Введите ваше имя: ")
last_name = input("Введите вашу фамилию: ")

# Создаем приветственное сообщение
hello = f"{first_name} {last_name}, приветствуем вас на курсе Data Engineer!"

# Сохраняем сообщение в файл
with open("/app/result/hello.txt", "w") as file:
    file.write(hello)

print("Cообщение для Вас сохранено в /app/result/hello.txt")