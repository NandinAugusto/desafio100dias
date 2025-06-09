

def area_circulo(raio):
    pi = 3.14159
    area = pi * (raio ** 2)
    return area


raio = float(input("Digite o raio do círculo: "))
area = area_circulo(raio)
print(f"A área do círculo é: {area:.2f}")

def f_para_c(f):
    c = (f - 32) * 5/9
    return c
fahrenheit = float(input("Digite a temperatura em Fahrenheit: "))
celsius = f_para_c(fahrenheit)
print(f"A temperatura em Celsius é: {celsius:.2f}")

def numero_par(numero):
    return numero % 2 == 0

x = int(input("Digite um número: "))
y = "é" if numero_par(x) else "não é"
print(f"O número {x} {y} par")

