"""Word Count - MapReduce"""

import glob
import os
import string
import time


def copy_raw_files_to_input_folder(n):
    """
    Copia n veces los archivos de files/raw a files/input.
    
    Args:
        n: Número de veces que se replican los archivos.
    """
    # Crea la carpeta files/input
    if os.path.exists("files/input/"):
        for file in glob.glob("files/input/*"):
            os.remove(file)
    else:
        os.makedirs("files/input")

    # Copia n veces los archivos de files/raw a files/input
    for file in glob.glob("files/raw/*"):

        with open(file, "r", encoding="utf-8") as f:
            text = f.read()

        for i in range(1, n + 1):
            raw_filename_with_extension = os.path.basename(file)
            raw_filename_without_extension = os.path.splitext(raw_filename_with_extension)[
                0
            ]
            new_filename = f"{raw_filename_without_extension}_{i}.txt"
            with open(f"files/input/{new_filename}", "w", encoding="utf-8") as f2:
                f2.write(text)


def run_job(input_dir, output_dir):
    """
    Ejecuta el job de word count (MapReduce).
    
    Args:
        input_dir: Directorio de entrada con los archivos a procesar.
        output_dir: Directorio de salida para los resultados.
    """
    # El experimento realmente empieza en este punto.
    start_time = time.time()

    # Lee los archivos de files/input
    sequence = []
    files = glob.glob(f"{input_dir}/*")
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                sequence.append((file, line))

    # Mapea las líneas a pares (palabra, 1). Este es el mapper.
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend([(word, 1) for word in words])

    # Ordena la secuencia de pares por la palabra. Este es el shuffle and sort.
    pairs_sequence = sorted(pairs_sequence)

    # Reduce la secuencia de pares sumando los valores por cada palabra. Este es el reducer.
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))

    # Crea la carpeta files/output
    if os.path.exists(output_dir):
        for file in glob.glob(f"{output_dir}/*"):
            os.remove(file)
    else:
        os.makedirs(output_dir)

    # Guarda el resultado en un archivo files/output/part-00000
    with open(f"{output_dir}/part-00000", "w", encoding="utf-8") as f:
        for key, value in result:
            f.write(f"{key}\t{value}\n")

    # Crea el archivo _SUCCESS en files/output
    with open(f"{output_dir}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")

    # El experimento finaliza aquí.
    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")