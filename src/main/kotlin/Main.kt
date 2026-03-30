package app

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PqlServerApplication

fun main(args: Array<String>) {
    runApplication<PqlServerApplication>(*args)
    println("Serwer PQL REST API uruchomiony na porcie 8080!")
}