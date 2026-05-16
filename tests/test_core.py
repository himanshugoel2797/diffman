"""Core library unit tests: Config, Variant, VariantRegistry, fingerprint."""

from __future__ import annotations

import pytest

from diffman.core import (
    Config, Variant, VariantRegistry, fingerprint,
    _deep_merge_into,
)


def test_config_deep_merge_attribute_access():
    cfg = Config({'scan': {'width': 5e-6, 'step': 1e-7}})
    assert cfg.scan.width == 5e-6
    cfg2 = Config(cfg, scan={'width': 8e-6})
    assert cfg2.scan.width == 8e-6     #override
    assert cfg2.scan.step == 1e-7      #merged from base
    assert cfg.scan.width == 5e-6      #original untouched


def test_config_attribute_assignment_promotes_to_config():
    cfg = Config()
    cfg.scan = {'width': 1e-6}
    assert isinstance(cfg['scan'], Config)
    assert cfg.scan.width == 1e-6


def test_fingerprint_is_stable_and_canonical():
    a = fingerprint({'x': 1, 'y': 2})
    b = fingerprint({'y': 2, 'x': 1})
    assert a == b
    assert a != fingerprint({'x': 1, 'y': 3})


def test_fingerprint_handles_non_json_with_default_str():
    #Non-JSON-serializable falls back to str(); must not raise.
    class Opaque:
        def __repr__(self):
            return '<opaque>'
    fp = fingerprint({'k': Opaque()})
    assert isinstance(fp, str) and len(fp) == 64


class TestVariantRegistry:
    def test_register_keys_on_module_and_name(self):
        r = VariantRegistry()
        r.register('base', module='m1', x=1)
        r.register('base', module='m2', x=2)  #same name, different module OK
        assert r.get('m1', 'base').config.x == 1
        assert r.get('m2', 'base').config.x == 2

    def test_register_collision_within_module_raises(self):
        r = VariantRegistry()
        r.register('base', module='m1', x=1)
        with pytest.raises(ValueError, match='already registered'):
            r.register('base', module='m1', x=2)

    def test_register_with_base_prefers_same_module(self):
        r = VariantRegistry()
        r.register('base', module='m1', x=1)
        r.register('base', module='m2', x=99)
        r.register('child', module='m1', base='base', y=2)
        assert r.get('m1', 'child').config.x == 1   #m1's base, not m2's

    def test_register_with_base_falls_back_cross_module_if_unambiguous(self):
        r = VariantRegistry()
        r.register('base', module='m_parent', x=10)
        r.register('child', module='m_child', base='base', y=20)
        assert r.get('m_child', 'child').config.x == 10

    def test_register_with_missing_base_raises(self):
        r = VariantRegistry()
        with pytest.raises(KeyError, match='not found'):
            r.register('child', module='m1', base='ghost')

    def test_register_with_ambiguous_base_raises(self):
        r = VariantRegistry()
        r.register('base', module='a', x=1)
        r.register('base', module='b', x=2)
        with pytest.raises(KeyError, match='ambiguous'):
            r.register('child', module='c', base='base')

    def test_for_module_filters_by_attribution(self):
        r = VariantRegistry()
        r.register('a', module='m1', x=1)
        r.register('b', module='m1', x=2)
        r.register('a', module='m2', x=3)
        assert sorted(r.for_module('m1')) == ['a', 'b']
        assert r.for_module('m2') == ['a']

    def test_drop_module_removes_only_that_modules_variants(self):
        r = VariantRegistry()
        r.register('a', module='m1', x=1)
        r.register('a', module='m2', x=2)
        r.drop_module('m1')
        assert r.for_module('m1') == []
        assert r.for_module('m2') == ['a']

    def test_register_records_forks_of(self):
        r = VariantRegistry()
        v = r.register('jitter_new', module='child', forks_of='jitter_old', x=1)
        assert v.forks_of == 'jitter_old'


class TestVariantConfig:
    def test_config_walks_base_chain_with_deep_merge(self):
        r = VariantRegistry()
        r.register('a', module='m', scan={'width': 1, 'step': 2})
        r.register('b', module='m', base='a', scan={'width': 99}, probe={'on': True})
        cfg = r.get('m', 'b').config
        assert cfg.scan.width == 99       #override
        assert cfg.scan.step == 2          #inherited
        assert cfg.probe.on is True       #added

    def test_fingerprint_changes_with_overrides(self):
        r = VariantRegistry()
        r.register('a', module='m', x=1)
        r.register('b', module='m', x=2)
        assert r.get('m', 'a').fingerprint != r.get('m', 'b').fingerprint

    def test_short_fp_is_first_12_chars(self):
        r = VariantRegistry()
        v = r.register('a', module='m', x=1)
        assert v.short_fp == v.fingerprint[:12]


def test_deep_merge_into_promotes_plain_dicts():
    dst = Config()
    _deep_merge_into(dst, {'a': {'b': 1}})
    assert isinstance(dst['a'], Config)
    _deep_merge_into(dst, {'a': {'c': 2}})
    assert dst.a.b == 1 and dst.a.c == 2
