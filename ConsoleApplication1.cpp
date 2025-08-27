
#include <iostream>
#include <thread>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <string>
#include <cctype>
#include <sstream>
#include <mutex>
#include <algorithm>
#include <atomic>
#include <chrono>

#include <future>
#include <iterator>   
#include <utility>    
#include <ppl.h>   




#define ll long long
#define MAXPRINT 100
#define BATCHSIZE 1024  // tamaño de bloque en MB

using namespace std;
using namespace std::chrono;

struct palabra {
    ll count = 0;
    vector<ll> positions;

    palabra() { positions.reserve(16); }
};



void guardarResultados(const std::string& nombreArchivo,
    const std::unordered_map<std::string, palabra>& finalMap,
    size_t totalProcesadas)
{
    std::ofstream archivo(nombreArchivo);
    if (!archivo.is_open()) {
        std::cerr << "Error: no se pudo abrir el archivo " << nombreArchivo << std::endl;
        return;
    }

    archivo << "=== RESULTADOS ===" << std::endl;
    archivo << "Total de palabras únicas: " << finalMap.size() << std::endl;
    archivo << "Total de palabras procesadas: " << totalProcesadas << std::endl;
    archivo << "\nPalabras encontradas:" << std::endl;

    for (const auto& pair : finalMap) {
        const auto& word = pair.first;
        const auto& info = pair.second;

        archivo << word << " " << info.count << " ";

        for (size_t j = 0; j <  info.positions.size(); j++) {
            archivo << info.positions[j];
            if (j < std::min(size_t(10), info.positions.size()) - 1) archivo << ",";
        }
        if (info.positions.size() > 10) archivo << "...";

        archivo << std::endl;
    }

    archivo.close();
    std::cout << "Resultados guardados en: " << nombreArchivo << std::endl;
}


struct string_views {
    const char* data_;
    size_t size_;

    // Constructores
    string_views() : data_(nullptr), size_(0) {}
    string_views(const char* s) : data_(s), size_(std::strlen(s)) {}
    string_views(const char* s, size_t count) : data_(s), size_(count) {}
    string_views(const std::string& s) : data_(s.data()), size_(s.size()) {}

    // Acceso
    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    const char& operator[](size_t i) const { return data_[i]; }

    // Iteradores
    const char* begin() const { return data_; }
    const char* end() const { return data_ + size_; }

    string_views substr(size_t pos, size_t count = std::string::npos) const {
        if (pos > size_) pos = size_;
        if (count > size_ - pos) count = size_ - pos;
        return string_views(data_ + pos, count);
    }


    std::string to_string() const { return std::string(data_, size_); }
};


atomic<ll> nextWordPosition(1);


static unsigned char TOLOWER_LUT[256];
static unsigned char ISALNUM_LUT[256];

inline void initLookup() {
    for (int i = 0; i < 256; ++i) {
        TOLOWER_LUT[i] = static_cast<unsigned char>(std::tolower(i));
        ISALNUM_LUT[i] = static_cast<unsigned char>(std::isalnum(i) ? 1 : 0);
    }
}

void contarP(string_views subtext,
    unordered_map<string, palabra>& localMap,
    ll& localWordCount) {
    const char* ptr = subtext.data();
    const char* end = ptr + subtext.size();

    localWordCount = 0;

    while (ptr < end) {

        while (ptr < end && std::isspace(static_cast<unsigned char>(*ptr))) ++ptr;
        if (ptr >= end) break;
        const char* wordStart = ptr;

        while (ptr < end && !std::isspace(static_cast<unsigned char>(*ptr))) ++ptr;


        string word;
        word.reserve(static_cast<size_t>(ptr - wordStart));
        for (const char* c = wordStart; c < ptr; ++c) {
            unsigned char uc = static_cast<unsigned char>(*c);
            if (ISALNUM_LUT[uc]) {
                word.push_back(static_cast<char>(TOLOWER_LUT[uc]));
            }
        }

        if (!word.empty()) {
            auto& entry = localMap[word];
            ++entry.count;

            entry.positions.push_back(localWordCount++);
        }
    }
}

ll findWordBoundary(const string& buffer, ll position, bool searchLeft = true) {
    if (searchLeft) {
        while (position > 0 && !isspace(static_cast<unsigned char>(buffer[position - 1]))) --position;
    }
    else {
        while (position < static_cast<ll>(buffer.size()) &&
            !isspace(static_cast<unsigned char>(buffer[position]))) ++position;
    }
    return position;
}


inline void mergeInto(unordered_map<string, palabra>& dst,
    unordered_map<string, palabra>& src) {

    dst.reserve(dst.size() + src.size());

    for (auto& kv : src) {
        const string& word = kv.first;
        palabra& info = kv.second;

        auto it = dst.find(word);
        if (it == dst.end()) {

            dst.emplace(word, std::move(info));
        }
        else {
      
            auto& entry = it->second;
            entry.count += info.count;
            auto& pos = entry.positions;
            pos.reserve(pos.size() + info.positions.size());
            move(info.positions.begin(), info.positions.end(),
                std::back_inserter(pos));
        }
    }

    src.clear();
}


int main(int argc, const char* argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    initLookup();

    string filename = "C://Users//Usuario//Documents//big_data//text.txt";
    ll segmentSizeMB = BATCHSIZE;
    ll segmentSize = segmentSizeMB * 1024 * 1024;

    ifstream file(filename, ios::binary | ios::ate);
    if (!file.is_open()) {
        cout << "Error: No se pudo abrir el archivo " << filename << endl;
        return 1;
    }

    ll fileSize = static_cast<ll>(file.tellg());
    file.seekg(0, ios::beg);

    cout << "Tamaño de archivo " << fileSize << " bytes en segmentos de "
        << segmentSize << " bytes" << endl;

    unsigned maxThreads = thread::hardware_concurrency();
    //if (maxThreads == 0) maxThreads = 4;
    cout << "Usando " << maxThreads << " threads" << endl;


    unordered_map<string, palabra> finalMap;
    finalMap.reserve(200000);

    ll processedBytes = 0;
    ll segmentNumber = 0;

    auto inicio = high_resolution_clock::now();


    vector<unordered_map<string, palabra>> threadMaps;
    threadMaps.resize(maxThreads);
    for (auto& m : threadMaps) m.reserve(50000);

    vector<thread> threads;
    threads.reserve(maxThreads);

    vector<ll> localCounts(maxThreads, 0);

    while (processedBytes < fileSize) {
        segmentNumber++;
        ll currentSegmentSize = min(segmentSize, fileSize - processedBytes);

        string buffer;
        buffer.resize(static_cast<size_t>(currentSegmentSize));
        file.read(&buffer[0], currentSegmentSize);

        if (processedBytes + currentSegmentSize < fileSize) {
            ll adjustedSize = findWordBoundary(buffer, currentSegmentSize, true);
            buffer.resize(static_cast<size_t>(adjustedSize));
            file.seekg(processedBytes + adjustedSize);
            currentSegmentSize = adjustedSize;
        }

        cout << "Procesando segmento " << segmentNumber << " ("
            << currentSegmentSize << " bytes)" << endl;


        for (auto& m : threadMaps) m.clear();

        threads.clear();
        ll bytesPerThread = (maxThreads > 0) ? (currentSegmentSize / maxThreads) : currentSegmentSize;

        for (unsigned i = 0; i < maxThreads; ++i) {
            ll start = static_cast<ll>(i) * bytesPerThread;
            ll end = (i == maxThreads - 1) ? currentSegmentSize : static_cast<ll>(i + 1) * bytesPerThread;

            if (i > 0) start = findWordBoundary(buffer, start, false);
            if (i < maxThreads - 1) end = findWordBoundary(buffer, end, true);

            if (start < end) {
                string_views sv(buffer.data() + start, static_cast<size_t>(end - start));
                threads.emplace_back(contarP, sv, ref(threadMaps[i]), ref(localCounts[i]));
            }
            else {
                localCounts[i] = 0;
            }
        }

        for (auto& t : threads) t.join();


        for (unsigned i = 0; i < maxThreads; ++i) {
            ll cnt = localCounts[i];
            if (cnt == 0) continue;

            ll base = nextWordPosition.fetch_add(cnt);
            ll delta = base - 1;

            
            auto& lm = threadMaps[i];
            for (auto& kv : lm) {
                auto& vec = kv.second.positions;
                for (auto& p : vec) p += delta;
            }
        }

       
        size_t active = maxThreads;
        
        while (active > 0 && threadMaps[active - 1].empty()) --active;

        size_t step = 1;
        while (step < active) {
            for (size_t i = 0; i + step < active; i += 2 * step) {
                mergeInto(threadMaps[i], threadMaps[i + step]);
                threadMaps[i + step].clear();
            }
            step *= 2;
            
            while (active > 0 && threadMaps[active - 1].empty()) --active;
        }

        if (active > 0) {
            mergeInto(finalMap, threadMaps[0]);
            threadMaps[0].clear();
        }

        processedBytes += currentSegmentSize;

        cout << "Progreso: " << (processedBytes * 100 / fileSize) << "% - "
            << "Palabras procesadas: " << (nextWordPosition.load() - 1) << endl;
    }

    file.close();
    auto finProcesamiento = high_resolution_clock::now();
    auto duracionProcesamiento = duration_cast<milliseconds>(finProcesamiento - inicio);

    cout << "Tiempo total de procesamiento (con merges parciales optimizados): "
        << duracionProcesamiento.count() << " ms" << endl;

    cout << "\n=== ÍNDICE INVERTIDO GENERADO ===" << endl;
    cout << "Total de palabras únicas: " << finalMap.size() << endl;
    cout << "Total de palabras procesadas: " << nextWordPosition.load() - 1 << endl;
    vector<pair<const string*, const palabra*>> sortedWords;
    sortedWords.reserve(finalMap.size());
    for (auto& kv : finalMap) {
        sortedWords.emplace_back(&kv.first, &kv.second);
    }

    // Ordenar por cantidad 
    


    int num_threads = std::thread::hardware_concurrency();

    size_t n = sortedWords.size();
    size_t block_size = (n + num_threads - 1) / num_threads;

    
    concurrency::parallel_for(0, num_threads, [&](int t) {
        size_t start = t * block_size;
        size_t end = std::min(start + block_size, n);
        if (start < end) {
            std::sort(sortedWords.begin() + start, sortedWords.begin() + end,
                [](const auto& a, const auto& b) {
                    return a.second->count > b.second->count;
                });
        }
        });

   
    for (size_t size = block_size; size < n; size *= 2) {
        for (size_t start = 0; start + size < n; start += 2 * size) {
            size_t mid = start + size;
            size_t end = std::min(start + 2 * size, n);
            std::inplace_merge(sortedWords.begin() + start,
                sortedWords.begin() + mid,
                sortedWords.begin() + end,
                [](const auto& a, const auto& b) {
                    return a.second->count > b.second->count;
                });
        }
    }




    cout << "\nPrimeras " << MAXPRINT << " palabras del índice :" << endl;
    size_t printed = 0;
    for (const auto& pair : sortedWords) {
        if (printed >= MAXPRINT) break;
        const auto& word = *pair.first;
        const auto& info = *pair.second;
        cout << word << " " << info.count << " ";
        for (size_t j = 0; j < std::min<size_t>(10, info.positions.size()); ++j) {
            cout << info.positions[j];
            if (j + 1 < std::min<size_t>(10, info.positions.size())) cout << ",";
        }
        if (info.positions.size() > 10) cout << "...";
        cout << "\n";
        ++printed;
    }


    ofstream archivo("C://Users//Usuario//Documents//big_data//resultado.txt");
    if (archivo.is_open()) {
        archivo << "=== RESULTADOS ===\n";
        archivo << "Total de palabras únicas: " << sortedWords.size() << "\n";
        archivo << "Total de palabras procesadas: " << nextWordPosition.load() - 1 << "\n";
        archivo << "\nPalabras encontradas :\n";

        for (const auto& pair : sortedWords) {
            const auto& word = *pair.first;
            const auto& info = *pair.second;

            archivo << word << " " << info.count << " ";
            for (size_t j = 0; j < std::min<size_t>(10, info.positions.size()); ++j) {
                archivo << info.positions[j];
                if (j + 1 < std::min<size_t>(10, info.positions.size())) archivo << ",";
            }
            if (info.positions.size() > 10) archivo << "...";
            archivo << "\n";
        }

        archivo.close();
        cout << "Resultados guardados en: C://Users//Usuario//Documents//big_data//resultado.txt" << endl;
    }
}
