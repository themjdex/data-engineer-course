object TaskStringProcessor extends App { /* extends App добавлен, так как без него не запускался код в IDE */
  // создаем чистую функцию проверки того, длиннее ли строка трех символов
  def checkLength(string: String): Boolean = string.length > 3

  // создаем чистую функцию смены регистра слова
  def upperCase(string: String): String = string.toUpperCase

  // заводим список слов для обработки
  val words: List[String] = List("apple", "cat", "banana", "dog", "elephant")

  // создаем функцию, которая переводит в верхний регистр все слова длиннее 3 символов
  def transformText(strings: List[String]): Unit = {
    // фильтруем строки в списке, используя чистую функцию
    val filterList: List[String] = strings.filter(checkLength)
    // применяем изменения ко всем отфильтрованным словам, используя другую чистую функцию
    val result = filterList.map(upperCase)

    println(s"Processed strings: $result")
  }
  // запускаем всю логику
  transformText(words)
}





