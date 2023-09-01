package br.com.fiap.domain.service;

import br.com.fiap.domain.entity.Turma;
import br.com.fiap.domain.repository.TurmaRepository;
import br.com.fiap.exception.MetodoNaoImplementadoException;

import java.util.List;

public class TurmaService implements Service<Turma, Long> {
    private TurmaRepository repository;

    public TurmaService() {
        repository =  TurmaRepository.of();
    }
    @Override
    public List<Turma> findAll() {
        return repository.findAll();
    }

    @Override
    public Turma findById(Long id) {
        return repository.findById(id);
    }

    @Override
    public List<Turma> findByName(String texto) {
        return repository.findByName(texto);
    }

    @Override
    public Turma persist(Turma turma) {
        return repository.persist(turma);
    }
}
